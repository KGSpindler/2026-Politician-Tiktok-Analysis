#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape Altinget FT26 candidate profiles from:
https://www.altinget.dk/kandidattest/FT26/profil/001
...
https://www.altinget.dk/kandidattest/FT26/profil/984

Collect:
- profile_id
- url
- candidate_name
- party
- age
- education

Notes
-----
This scraper is built to work without a browser. The uploaded example page is
server-rendered and contains both:
1) visible profile fields in the HTML, and
2) a __NUXT_DATA__ payload with fields such as Birthdate, CurrentParty and Education.

The scraper therefore uses:
- visible HTML as the primary parser
- __NUXT_DATA__ as a fallback
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from datetime import date
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Iterable

import requests
from bs4 import BeautifulSoup, Tag


BASE_URL = "https://www.altinget.dk/kandidattest/FT26/profil/{profile_id:03d}"
DEFAULT_OUT = "altinget_ft26_candidates.csv"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


@dataclass
class CandidateRow:
    profile_id: int
    url: str
    status: str
    candidate_name: Optional[str] = None
    party: Optional[str] = None
    age: Optional[str] = None
    education: Optional[str] = None
    birthdate: Optional[str] = None
    error: Optional[str] = None


def clean_text(value: object | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    value = re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()
    return value or None


def derive_age_from_birthdate(birthdate: str | None, today: date | None = None) -> str | None:
    if not birthdate:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", birthdate)
    if not m:
        return None

    year, month, day = map(int, m.groups())
    today = today or date.today()

    years = today.year - year
    if (today.month, today.day) < (month, day):
        years -= 1

    if years < 0:
        return None
    return str(years)


def iter_divs(soup: BeautifulSoup) -> Iterable[Tag]:
    for div in soup.find_all("div"):
        yield div


def find_candidate_name(soup: BeautifulSoup) -> Optional[str]:
    h1s = soup.find_all("h1")
    for h1 in h1s:
        text = clean_text(h1.get_text(" ", strip=True))
        if text and text.lower() != "kandidattesten":
            return text
    return None


def find_party(soup: BeautifulSoup) -> Optional[str]:
    # Pattern in uploaded page:
    # <span class="italic opacity-50">for</span><span class="headline-2xs !font-normal">Uden for parti</span>
    for span in soup.find_all("span"):
        text = clean_text(span.get_text(" ", strip=True))
        if text == "for":
            nxt = span.find_next_sibling("span")
            if nxt:
                party = clean_text(nxt.get_text(" ", strip=True))
                if party:
                    return party
    return None


def find_labeled_value(soup: BeautifulSoup, label: str) -> Optional[str]:
    label = label.rstrip(":").lower()
    for div in iter_divs(soup):
        text = clean_text(div.get_text(" ", strip=True))
        if not text:
            continue
        lower = text.lower()
        if lower.startswith(f"{label}:"):
            value = clean_text(text.split(":", 1)[1])
            if value:
                return value
    return None


def extract_nuxt_json(html: str) -> Optional[object]:
    m = re.search(
        r'<script[^>]+id="__NUXT_DATA__"[^>]*>(.*?)</script>',
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return None
    raw = m.group(1).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def find_candidate_blob(obj: object, profile_id: int) -> Optional[dict]:
    """
    Search recursively for the object that contains:
    - ID == profile_id
    - Firstname / LastName
    """
    if isinstance(obj, dict):
        if obj.get("ID") == profile_id and ("Firstname" in obj or "LastName" in obj):
            return obj
        for value in obj.values():
            found = find_candidate_blob(value, profile_id)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_candidate_blob(item, profile_id)
            if found:
                return found
    return None


def extract_from_nuxt(html: str, profile_id: int) -> dict:
    """
    Fallback extractor.

    The uploaded file contains devalued Nuxt state rather than a neat flat JSON document.
    Instead of fully resolving the whole devalue graph, we use a targeted regex fallback
    against the embedded payload.

    Example from the uploaded page:
    ..."Søren Skjold","Andersen",...,"1985-10-10T00:00:00",...,"Uden for parti",...,"Kandidat-/masteruddannelse"...
    """
    out = {
        "candidate_name": None,
        "party": None,
        "birthdate": None,
        "education": None,
        "age": None,
    }

    nuxt = extract_nuxt_json(html)
    if isinstance(nuxt, (list, dict)):
        blob = find_candidate_blob(nuxt, profile_id)
        if isinstance(blob, dict):
            first = clean_text(blob.get("Firstname"))
            last = clean_text(blob.get("LastName"))
            if first or last:
                out["candidate_name"] = clean_text(" ".join(x for x in [first, last] if x))
            out["party"] = clean_text(blob.get("CurrentParty")) or clean_text(blob.get("LinedUpForParty"))
            out["birthdate"] = clean_text(blob.get("Birthdate"))
            out["education"] = clean_text(blob.get("Education"))

    # Regex fallback if the recursive search fails because of Nuxt references/devalue encoding
    if not out["candidate_name"] or not out["party"] or not out["education"]:
        m = re.search(
            rf'"vaa-candidate-[^"]*?{profile_id}"[^[]*?\{{"ID":\d+,"Firstname":\d+,"LastName":\d+.*?"Birthdate":\d+.*?"CurrentParty":\d+.*?"Education":\d+.*?\}},'
            rf'{profile_id},'
            r'"([^"]+)","([^"]+)",'
            r'"[^"]*",'
            r'"[^"]*",'
            r'"[^"]*",'
            r'"([^"]+)",'
            r'"[^"]*",'
            r'null,'
            r'"([^"]*)",'
            r'"[^"]*",'
            r'"[^"]*",'
            r'"[^"]*",'
            r'"[^"]*",'
            r'"[^"]*",'
            r'"([^"]*)"',
            html,
            flags=re.DOTALL,
        )
        if m:
            first, last, birthdate, party, education = m.groups()
            out["candidate_name"] = out["candidate_name"] or clean_text(f"{first} {last}")
            out["party"] = out["party"] or clean_text(party)
            out["birthdate"] = out["birthdate"] or clean_text(birthdate)
            out["education"] = out["education"] or clean_text(education)

    return out


def parse_profile_html(html: str, profile_id: int, url: str) -> CandidateRow:
    soup = BeautifulSoup(html, "html.parser")

    name = find_candidate_name(soup)
    party = find_party(soup)
    age = find_labeled_value(soup, "Alder")
    education = find_labeled_value(soup, "Uddannelse")

    fallback = extract_from_nuxt(html, profile_id)

    row = CandidateRow(
        profile_id=profile_id,
        url=url,
        status="ok",
        candidate_name=name or fallback.get("candidate_name"),
        party=party or fallback.get("party"),
        age=age or fallback.get("age"),
        education=education or fallback.get("education"),
        birthdate=fallback.get("birthdate"),
    )

    # If the visible page is missing but Nuxt gives us birthdate, derive age only if needed.
    # We preserve rendered values such as "40 år" when available.
    if not row.age and row.birthdate:
        row.age = derive_age_from_birthdate(row.birthdate)

    if not row.candidate_name:
        row.status = "missing_candidate"
        row.error = "Could not locate candidate name on page"

    return row


def fetch_html(session: requests.Session, url: str, timeout: int) -> tuple[int, str]:
    resp = session.get(url, timeout=timeout)
    return resp.status_code, resp.text


def scrape_profiles(
    start_id: int,
    end_id: int,
    out_csv: Path,
    sleep_seconds: float,
    timeout: int,
) -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    rows: list[CandidateRow] = []

    for profile_id in range(start_id, end_id + 1):
        url = BASE_URL.format(profile_id=profile_id)
        print(f"[{profile_id:03d}] {url}")
        try:
            status_code, html = fetch_html(session, url, timeout=timeout)

            if status_code != 200:
                rows.append(
                    CandidateRow(
                        profile_id=profile_id,
                        url=url,
                        status=f"http_{status_code}",
                        error=f"HTTP {status_code}",
                    )
                )
            else:
                row = parse_profile_html(html, profile_id=profile_id, url=url)
                rows.append(row)

        except requests.RequestException as exc:
            rows.append(
                CandidateRow(
                    profile_id=profile_id,
                    url=url,
                    status="request_error",
                    error=f"{type(exc).__name__}: {exc}",
                )
            )

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    write_rows(out_csv, rows)
    print(f"\nSaved {len(rows)} rows to {out_csv.resolve()}")


def write_rows(path: Path, rows: list[CandidateRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "profile_id",
        "url",
        "status",
        "candidate_name",
        "party",
        "age",
        "education",
        "birthdate",
        "error",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-id", type=int, default=1)
    parser.add_argument("--end-id", type=int, default=984)
    parser.add_argument("--out", type=Path, default=Path(DEFAULT_OUT))
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    scrape_profiles(
        start_id=args.start_id,
        end_id=args.end_id,
        out_csv=args.out,
        sleep_seconds=args.sleep,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
