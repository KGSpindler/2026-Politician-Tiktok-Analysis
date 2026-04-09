#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
valg.dk Folketing scraper

Goal
----
Scrape candidate results for every party in every polling area (afstemningsområde /
opstillingskreds subtree) for a Folketing election page such as:

https://valg.dk/fv/47b883ef-d0d3-4cb6-9da5-91963f0e9ba0

Pipeline
--------
1. Open election page
2. Expand the "Gå til opstillingskreds" section
3. Expand each storkreds button
4. Collect every nomination-district link
5. Visit each nomination-district page
6. Collect every polling-district link
7. Visit each polling-district page
8. Expand each party block
9. Extract party-level and candidate-level rows in displayed order
10. Save CSV files

Requirements
------------
pip install playwright pandas beautifulsoup4 lxml
python -m playwright install chromium

Run
---
python valgdk_folketing_scraper.py

Optional custom URL:
python valgdk_folketing_scraper.py "https://valg.dk/fv/47b883ef-d0d3-4cb6-9da5-91963f0e9ba0"

Notes
-----
- This is built to follow the actual UI structure you described:
  * storkreds buttons like: <button ...> Bornholm </button>
  * nomination links like: <a href="/fv/.../nomination-district/...">1. Rønne</a>
- The polling-district pages are JavaScript pages, so Playwright is used.
- The exact CSS classes for the result cards can change. The script therefore combines:
  1) explicit link/expand logic, and
  2) robust HTML-table / DOM-order fallback parsing.
"""

import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
try:
    from playwright.sync_api import Error as PlaywrightError  # type: ignore[import-not-found]
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError  # type: ignore[import-not-found]
    from playwright.sync_api import sync_playwright  # type: ignore[import-not-found]
    _PLAYWRIGHT_IMPORT_ERROR = None
except ImportError as exc:
    PlaywrightError = Exception
    PlaywrightTimeoutError = TimeoutError
    sync_playwright = None
    _PLAYWRIGHT_IMPORT_ERROR = exc


START_URL = "https://valg.dk/fv/847cec19-05ff-4eff-b433-f0e886b24b7d"
HEADLESS = True
ROOT = Path(__file__).resolve().parents[1]
RUN_ID = datetime.now().strftime("%Y-%m-%d_%H%M%S")
OUTDIR = Path(os.environ.get("VALGDK_OUTDIR", str(ROOT / "Outputs" / "Valgdk" / f"run_{RUN_ID}")))

NOMINATION_LINKS_CSV = "nomination_district_links.csv"
POLLING_LINKS_CSV = "polling_district_links.csv"
PARTY_RESULTS_CSV = "polling_district_party_results.csv"
CANDIDATE_RESULTS_CSV = "polling_district_candidate_results.csv"
NAVIGATION_ATTEMPTS = 3
NAVIGATION_TIMEOUT_MS = 120000
NAVIGATION_RETRY_SLEEP_MS = 2500


def clean_text(value: str) -> str:
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def safe_slug(value: str) -> str:
    value = clean_text(value)
    value = re.sub(r"[^\w\-]+", "_", value, flags=re.UNICODE)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "page"


def to_abs(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


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


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: List[Dict], field_order: List[str] = None) -> None:
    ensure_dir(path.parent)
    if not rows:
        if field_order is None:
            field_order = ["empty"]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=field_order)
            writer.writeheader()
        return

    keys = []
    seen = set()
    if field_order:
        for k in field_order:
            if k not in seen:
                keys.append(k)
                seen.add(k)

    for row in rows:
        for k in row.keys():
            if k not in seen:
                keys.append(k)
                seen.add(k)

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def append_csv(path: Path, rows: List[Dict]) -> None:
    """Append rows to existing CSV file (without rewriting header)."""
    ensure_dir(path.parent)
    if not rows:
        return

    if not path.exists():
        write_csv(path, rows)
        return

    # Determine field order from existing file
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

    # Add any new fields from rows
    seen = set(fieldnames) if fieldnames else set()
    new_fields = []
    for row in rows:
        for k in row.keys():
            if k not in seen:
                new_fields.append(k)
                seen.add(k)

    fieldnames = list(fieldnames) + new_fields

    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerows(rows)


def load_csv(path: Path) -> List[Dict]:
    """Load existing CSV file."""
    if not path.exists():
        return []
    
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)


def save_html(page, path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(page.content(), encoding="utf-8")


def wait_settle(page, ms: int = 1200) -> None:
    page.wait_for_timeout(ms)


def goto_with_retry(page, url: str, attempts: int = NAVIGATION_ATTEMPTS) -> None:
    """Navigate with retries for transient valg.dk network errors."""
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


def click_expandable_buttons(page) -> int:
    """
    Expand storkreds / accordion / party blocks as safely as possible.
    """
    clicks = 0

    # First: explicit aria-expanded buttons
    try:
        buttons = page.locator("button")
        n = buttons.count()
        for i in range(n):
            try:
                btn = buttons.nth(i)
                text = clean_text(btn.inner_text())
                aria = (btn.get_attribute("aria-expanded") or "").lower()

                # Storkreds / party accordion buttons tend to be plain buttons with labels.
                if aria == "false":
                    btn.click(timeout=1500)
                    page.wait_for_timeout(120)
                    clicks += 1
                    continue

                # Fall back to likely text-bearing expanders
                if text and len(text) < 80:
                    if any(
                        hint in text.lower()
                        for hint in [
                            "bornholm",
                            "københavn",
                            "omegn",
                            "sjælland",
                            "fyn",
                            "østjylland",
                            "vestjylland",
                            "nordjylland",
                            "sydjylland",
                            "nordsjælland",
                        ]
                    ):
                        try:
                            btn.click(timeout=600)
                            page.wait_for_timeout(80)
                            clicks += 1
                        except Exception:
                            pass
            except Exception:
                pass
    except Exception:
        pass

    # Some pages use summary/details
    try:
        summaries = page.locator("summary")
        n = summaries.count()
        for i in range(n):
            try:
                summaries.nth(i).click(timeout=700)
                page.wait_for_timeout(80)
                clicks += 1
            except Exception:
                pass
    except Exception:
        pass

    return clicks


def collect_nomination_links(page, base_url: str) -> List[Dict]:
    """
    Expand the election landing page and collect every nomination-district link.
    """
    wait_settle(page, 1500)

    # Try to reveal the nomination district area
    possible_texts = [
        "Gå til opstillingskreds",
        "Opstillingskreds",
        "Vis alle",
    ]
    for txt in possible_texts:
        try:
            loc = page.get_by_text(txt, exact=False)
            for i in range(loc.count()):
                try:
                    loc.nth(i).click(timeout=1200)
                    page.wait_for_timeout(100)
                except Exception:
                    pass
        except Exception:
            pass

    click_expandable_buttons(page)
    wait_settle(page, 1000)

    rows = []
    seen = set()

    anchors = page.locator("a")
    for i in range(anchors.count()):
        try:
            a = anchors.nth(i)
            href = a.get_attribute("href") or ""
            text = clean_text(a.inner_text())
            if "/nomination-district/" in href:
                url = to_abs(base_url, href)
                if url not in seen:
                    seen.add(url)
                    rows.append(
                        {
                            "nomination_label": text,
                            "nomination_url": url,
                        }
                    )
        except Exception:
            pass

    rows.sort(key=lambda r: r["nomination_label"])
    return rows


def collect_polling_links(page, nomination_url: str, nomination_label: str) -> List[Dict]:
    """
    From a nomination-district page, collect every polling-district link.
    """
    wait_settle(page, 1000)

    rows = []
    seen = set()

    anchors = page.locator("a")
    for i in range(anchors.count()):
        try:
            a = anchors.nth(i)
            href = a.get_attribute("href") or ""
            text = clean_text(a.inner_text())
            if "/polling-district/" in href:
                url = to_abs(nomination_url, href)
                if url not in seen:
                    seen.add(url)
                    rows.append(
                        {
                            "nomination_label": nomination_label,
                            "nomination_url": nomination_url,
                            "polling_label": text,
                            "polling_url": url,
                        }
                    )
        except Exception:
            pass

    rows.sort(key=lambda r: r["polling_label"])
    return rows


def expand_all_party_blocks(page) -> None:
    """
    On a polling-district page, expand every party result block before scraping candidates.
    """
    wait_settle(page, 1200)

    # First click any collapsed buttons with aria-expanded=false
    try:
        buttons = page.locator("button")
        n = buttons.count()
        for i in range(n):
            try:
                btn = buttons.nth(i)
                aria = (btn.get_attribute("aria-expanded") or "").lower()
                text = clean_text(btn.inner_text())
                if aria == "false":
                    btn.click(timeout=1200)
                    page.wait_for_timeout(100)
                elif text and len(text) < 120:
                    # Many party headers are buttons as well
                    if re.match(r"^[A-ZÆØÅa-zæøå].+", text):
                        try:
                            btn.click(timeout=400)
                            page.wait_for_timeout(60)
                        except Exception:
                            pass
            except Exception:
                pass
    except Exception:
        pass

    # Some pages use headings or summary elements
    for selector in ["summary", "[role='button']"]:
        try:
            loc = page.locator(selector)
            n = loc.count()
            for i in range(n):
                try:
                    el = loc.nth(i)
                    text = clean_text(el.inner_text())
                    if text and len(text) < 120:
                        el.click(timeout=400)
                        page.wait_for_timeout(50)
                except Exception:
                    pass
        except Exception:
            pass

    wait_settle(page, 800)


def parse_polling_page(html: str, polling_url: str, nomination_label: str, polling_label: str):
    """
    Generic DOM-order parser.

    Strategy:
    - Walk the DOM in document order
    - Find likely party headers
    - Within each party container or following sibling block, extract candidate rows
    - Preserve candidate ordering by DOM order
    - Also fall back to tables when present

    Because valg.dk is JS-rendered and may change structure, this parser is intentionally
    heuristic but ordered.
    """
    soup = BeautifulSoup(html, "html.parser")
    meta = parse_url_meta(polling_url)

    party_rows = []
    candidate_rows = []

    def cell_text(cells, index):
        if index >= len(cells):
            return ""
        return clean_text(cells[index].get_text(" ", strip=True))

    def split_party_name(text: str) -> str:
        text = clean_text(text)
        text = re.sub(r"^table\.expand_button\s*", "", text)
        text = re.sub(r"^table\.expand_all_button\s*", "", text)
        text = re.sub(r"^[A-ZÆØÅ]\.[\s\u00a0]*", "", text)
        text = re.sub(r"\s+Opstillingsform:.*$", "", text).strip()
        return text

    def extract_opstillingsform(text: str) -> str:
        match = re.search(r"Opstillingsform:\s*(.+)$", text)
        return clean_text(match.group(1)) if match else ""

    def parse_votes(text: str):
        text = clean_text(text)
        match = re.search(r"\d[\d\.]*", text)
        if not match:
            return None
        return int(match.group(0).replace(".", ""))

    parties_table = None
    for table in soup.find_all("table"):
        desc = table.get("aria-describedby") or ""
        if "valg-table-0-title" in desc:
            parties_table = table
            break

    if parties_table is not None:
        current_party = ""
        current_party_order = -1
        candidate_order = 0

        trs = parties_table.find_all("tr")
        for tr in trs[1:]:
            classes = tr.get("class") or []
            cells = tr.find_all(["td", "th"], recursive=False)
            if not cells:
                continue

            if "parent-row" in classes:
                current_party_order += 1
                candidate_order = 0

                party_label_text = cell_text(cells, 1)
                party_votes_text = cell_text(cells, 2)
                party_percentage_text = cell_text(cells, 3)

                current_party = split_party_name(party_label_text)
                party_rows.append(
                    {
                        "row_type": "party",
                        "party_name": current_party,
                        "party_order": current_party_order,
                        "party_votes": parse_votes(party_votes_text),
                        "party_percentage": party_percentage_text,
                        "opstillingsform": extract_opstillingsform(party_label_text),
                        "nomination_label": nomination_label,
                        "polling_label": polling_label,
                        "polling_url": polling_url,
                        **meta,
                    }
                )
                continue

            if "child-row" in classes and current_party:
                row_label = cell_text(cells, 1)
                votes_text = cell_text(cells, 2)
                percentage_text = cell_text(cells, 3)

                if not row_label:
                    continue

                if row_label == "Partistemmer":
                    party_rows.append(
                        {
                            "row_type": "party_list_votes",
                            "party_name": current_party,
                            "party_order": current_party_order,
                            "label": row_label,
                            "votes": parse_votes(votes_text),
                            "percentage": percentage_text,
                            "nomination_label": nomination_label,
                            "polling_label": polling_label,
                            "polling_url": polling_url,
                            **meta,
                        }
                    )
                    continue

                candidate_order += 1
                candidate_rows.append(
                    {
                        "row_type": "candidate",
                        "party_name": current_party,
                        "party_order": current_party_order,
                        "candidate_order_within_party": candidate_order,
                        "candidate_name": row_label,
                        "votes": parse_votes(votes_text),
                        "percentage": percentage_text,
                        "nomination_label": nomination_label,
                        "polling_label": polling_label,
                        "polling_url": polling_url,
                        **meta,
                    }
                )

        if party_rows or candidate_rows:
            return party_rows, candidate_rows

    # Fallback: best-effort DOM scan if the party table structure changes.
    elements = soup.find_all(["button", "h1", "h2", "h3", "h4", "h5", "h6", "div", "li", "p", "tr"])
    current_party = ""
    current_party_order = -1
    candidate_order = 0
    seen_party_headers = set()

    def looks_like_party_header(text: str) -> bool:
        if not text:
            return False
        t = clean_text(text)
        if len(t) > 120:
            return False
        if any(x in t.lower() for x in ["stemme", "valgsted", "afstemningsområde", "opstillingskreds"]):
            return False
        return any(
            k in t.lower()
            for k in [
                "socialdemokrat",
                "radikale",
                "konservative",
                "socialistisk folkeparti",
                "liberal alliance",
                "moderaterne",
                "dansk folkeparti",
                "venstre",
                "danmarksdemokraterne",
                "enhedslisten",
                "alternativet",
                "borgernes parti",
            ]
        )

    def looks_like_candidate_line(text: str) -> bool:
        t = clean_text(text)
        if not t or len(t) > 250:
            return False
        words = t.split()
        alpha_words = [w for w in words if re.search(r"[A-Za-zÆØÅæøå]", w)]
        return len(alpha_words) >= 2 and bool(re.search(r"\d", t))

    def parse_candidate_text(text: str) -> Dict:
        t = clean_text(text)
        nums = re.findall(r"\d[\d\.,]*", t)
        m = re.match(r"^(.*?)(?=\s+\d[\d\.,]*|\s*$)", t)
        candidate_name = clean_text(m.group(1)) if m else t

        row = {
            "candidate_name": candidate_name,
            "candidate_raw_text": t,
            "numeric_tokens": " | ".join(nums),
        }
        if len(nums) >= 1:
            row["candidate_votes_1"] = nums[0]
        if len(nums) >= 2:
            row["candidate_votes_2"] = nums[1]
        if len(nums) >= 3:
            row["candidate_votes_3"] = nums[2]
        return row

    for el in elements:
        text = clean_text(el.get_text(" ", strip=True))
        if not text:
            continue

        if looks_like_party_header(text):
            current_party = text
            if current_party not in seen_party_headers:
                current_party_order += 1
                seen_party_headers.add(current_party)
                party_rows.append(
                    {
                        "row_type": "party",
                        "party_name": current_party,
                        "party_order": current_party_order,
                        "nomination_label": nomination_label,
                        "polling_label": polling_label,
                        "polling_url": polling_url,
                        "party_header_text": text,
                        **meta,
                    }
                )
            continue

        if current_party and looks_like_candidate_line(text):
            candidate_order += 1
            parsed = parse_candidate_text(text)
            candidate_rows.append(
                {
                    "row_type": "candidate",
                    "party_name": current_party,
                    "party_order": current_party_order,
                    "candidate_order_within_party": candidate_order,
                    "nomination_label": nomination_label,
                    "polling_label": polling_label,
                    "polling_url": polling_url,
                    **meta,
                    **parsed,
                }
            )

    return party_rows, candidate_rows


def scrape(start_url: str, outdir: Path) -> None:
    if sync_playwright is None:
        raise RuntimeError(
            "Missing dependency: playwright. Install with `pip install playwright` and "
            "run `python -m playwright install chromium`."
        ) from _PLAYWRIGHT_IMPORT_ERROR

    ensure_dir(outdir)
    raw_dir = outdir / "raw_html"
    ensure_dir(raw_dir)

    nomination_rows = []
    polling_rows = []
    all_party_rows = []
    all_candidate_rows = []

    # Check if we're resuming from a checkpoint
    polling_links_path = outdir / POLLING_LINKS_CSV
    party_results_path = outdir / PARTY_RESULTS_CSV
    candidate_results_path = outdir / CANDIDATE_RESULTS_CSV
    
    resume_from_polling_index = 0
    if polling_links_path.exists():
        print("Resuming from checkpoint...")
        polling_rows = load_csv(polling_links_path)
        all_party_rows = load_csv(party_results_path)
        all_candidate_rows = load_csv(candidate_results_path)
        
        print(f"Loaded {len(polling_rows)} polling district links")
        print(f"Loaded {len(all_party_rows)} party result rows")
        print(f"Loaded {len(all_candidate_rows)} candidate result rows")
        
        # HARDCODED RESUME POINT FOR THIS RUN: Start from index 583
        resume_from_polling_index = 583
        print(f"\n*** HARDCODED RESUME POINT: Starting from index 583 (polling_0584) ***\n")
    else:
        # First run: collect nomination and polling links
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS)
            context = browser.new_context()

            # Election page
            page = context.new_page()
            goto_with_retry(page, start_url)
            wait_settle(page, 1800)
            save_html(page, raw_dir / "election_page.html")

            nomination_rows = collect_nomination_links(page, start_url)
            if not nomination_rows:
                raise RuntimeError("No nomination-district links found on the election page.")

            write_csv(outdir / NOMINATION_LINKS_CSV, nomination_rows, ["nomination_label", "nomination_url"])

            # Nomination pages
            for i, nd in enumerate(nomination_rows, start=1):
                nd_page = context.new_page()
                try:
                    goto_with_retry(nd_page, nd["nomination_url"])
                    wait_settle(nd_page, 1200)
                    save_html(nd_page, raw_dir / f"nomination_{i:03d}_{safe_slug(nd['nomination_label'])}.html")

                    nd_polling = collect_polling_links(
                        nd_page,
                        nd["nomination_url"],
                        nd["nomination_label"],
                    )
                    polling_rows.extend(nd_polling)
                finally:
                    nd_page.close()

            write_csv(
                outdir / POLLING_LINKS_CSV,
                polling_rows,
                ["nomination_label", "nomination_url", "polling_label", "polling_url"],
            )

            browser.close()

    # Polling pages (always run, either from start or from checkpoint)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()

        for i, pr in enumerate(polling_rows, start=1):
            # Skip already-processed polling districts
            if i <= resume_from_polling_index:
                continue

            pd_page = context.new_page()
            try:
                goto_with_retry(pd_page, pr["polling_url"])
                wait_settle(pd_page, 1500)

                # Important: expand party blocks before scraping candidates
                expand_all_party_blocks(pd_page)
                wait_settle(pd_page, 800)

                save_html(pd_page, raw_dir / f"polling_{i:04d}_{safe_slug(pr['polling_label'])}.html")
                html = pd_page.content()

                party_rows, candidate_rows = parse_polling_page(
                    html=html,
                    polling_url=pr["polling_url"],
                    nomination_label=pr["nomination_label"],
                    polling_label=pr["polling_label"],
                )

                all_party_rows.extend(party_rows)
                all_candidate_rows.extend(candidate_rows)
                
                # Periodically append to CSVs to preserve progress
                if len(party_rows) > 0:
                    append_csv(party_results_path, party_rows)
                if len(candidate_rows) > 0:
                    append_csv(candidate_results_path, candidate_rows)

            except (PlaywrightTimeoutError, PlaywrightError) as exc:
                error_row = {
                    "polling_url": pr["polling_url"],
                    "polling_label": pr["polling_label"],
                    "nomination_label": pr["nomination_label"],
                    "error": f"navigation_error: {type(exc).__name__}",
                }
                all_party_rows.append(error_row)
                append_csv(party_results_path, [error_row])
            finally:
                pd_page.close()

        browser.close()

    # Final write (in case we're resuming and need to write the final batch)
    if resume_from_polling_index == 0:
        write_csv(party_results_path, all_party_rows)
        write_csv(candidate_results_path, all_candidate_rows)

    print("Done.")
    print("Nomination districts:", len(nomination_rows) if nomination_rows else len(load_csv(outdir / NOMINATION_LINKS_CSV)))
    print("Polling districts:", len(polling_rows))
    print("Party rows:", len(all_party_rows))
    print("Candidate rows:", len(all_candidate_rows))
    print("Output folder:", outdir.resolve())


def main():
    start_url = sys.argv[1] if len(sys.argv) > 1 else START_URL
    scrape(start_url, OUTDIR)


if __name__ == "__main__":
    main()
