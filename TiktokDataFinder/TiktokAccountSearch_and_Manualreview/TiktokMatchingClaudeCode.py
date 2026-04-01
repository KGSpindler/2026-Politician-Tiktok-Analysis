#WRITTEN BY CLAUDE

"""
candidate_tiktok_search.py
──────────────────────────
Finds TikTok profiles for Danish Folketing candidates using DuckDuckGo
and Google in parallel, then verifies each profile is live.

Requirements:
    pip install pandas httpx tqdm duckduckgo-search

Usage:
    python candidate_tiktok_search.py
    python candidate_tiktok_search.py --input my_candidates.csv
    python candidate_tiktok_search.py --concurrency 10 --no-google
    python candidate_tiktok_search.py --reset
"""

import argparse
import asyncio
import collections
import csv
import json
import random
import re
import time
import unicodedata
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import httpx
import pandas as pd
from ddgs import DDGS
from tqdm.asyncio import tqdm as atqdm


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Find TikTok accounts for Danish Folketing candidates"
    )
    p.add_argument("--input",       default=r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\Candidate Dataset\candidates_list_2026.csv",
                   help="Path to candidates CSV (default: candidates_list_2026.csv)")
    p.add_argument("--concurrency", type=int, default=6,
                   help="Parallel workers (default: 6)")
    p.add_argument("--no-google",   action="store_true",
                   help="Skip Google fallback (faster, lower coverage)")
    p.add_argument("--reset",       action="store_true",
                   help="Ignore existing checkpoint and start fresh")
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

BASEDIR         = Path(r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\Candidate Tiktok Profile Matching + Quick Cleaning)
BASEDIR.mkdir(exist_ok=True)

RAW_OUT         = BASEDIR / "candidate_tiktok_raw.csv"
BEST_OUT        = BASEDIR / "candidate_tiktok_best.csv"
NO_MATCH_OUT    = BASEDIR / "candidate_tiktok_no_match.csv"
DUPLICATE_OUT   = BASEDIR / "candidate_tiktok_duplicates.csv"
CHECKPOINT_FILE = BASEDIR / "candidate_tiktok_checkpoint.json"

MAX_RESULTS      = 5
REQUEST_TIMEOUT  = 12
MAX_RETRIES      = 2
CHECKPOINT_EVERY = 20

HIGH_CONFIDENCE  = 75
POSSIBLE_MATCH   = 45
PATTERN_MIN_HITS = 3   # confirmed matches needed before trusting a party pattern

# Adaptive sleep — tuned at runtime by _record_ddg_success / _record_ddg_error
_sleep_min = 0.6
_sleep_max = 1.4
_ddg_errors    = 0
_ddg_successes = 0

# Party pattern feedback: {canonical_party -> Counter({pattern_name: count})}
_party_patterns: Dict[str, collections.Counter] = collections.defaultdict(collections.Counter)


# ─────────────────────────────────────────────────────────────────────────────
# PARTY TABLE
# ─────────────────────────────────────────────────────────────────────────────

# (canonical_name, csv_substrings, tiktok_priority, handle_abbrev)
PARTIES: List[Tuple[str, List[str], int, str]] = [
    ("liberal alliance",     ["liberal alliance"],                 10, "la"),
    ("alternativet",         ["alternativet"],                      9, "aa"),
    ("sf",                   ["socialistisk folkeparti", "sf -"],   9, "sf"),
    ("moderaterne",          ["moderaterne"],                       8, "m"),
    ("radikale venstre",     ["radikale venstre"],                  7, "b"),
    ("enhedslisten",         ["enhedslisten"],                      7, "oe"),
    ("socialdemokratiet",    ["socialdemokratiet"],                  6, "s"),
    ("konservative",         ["konservative folkeparti"],            6, "c"),
    ("venstre",              ["venstre"],                           5, "v"),
    ("danmarksdemokraterne", ["danmarksdemokraterne"],               5, "ae"),
    ("borgernes parti",      ["borgernes parti"],                   4, "d"),
    ("dansk folkeparti",     ["dansk folkeparti"],                  3, "df"),
    ("kristendemokraterne",  ["kristendemokraterne"],               2, "kd"),
    ("frie grønne",          ["frie grønne"],                       2, "q"),
    ("nye borgerlige",       ["nye borgerlige"],                    2, "nb"),
]

def _lookup_party(party_csv: str) -> Tuple[str, int, str]:
    pn = party_csv.lower()
    for canonical, substrings, priority, abbrev in PARTIES:
        if any(s in pn for s in substrings):
            return canonical, priority, abbrev
    return pn.strip(), 1, ""


# ─────────────────────────────────────────────────────────────────────────────
# TEXT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _norm(text: str) -> str:
    """Lowercase, collapse whitespace. Keeps æøå intact."""
    if not isinstance(text, str) or not text.strip():
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())

def _ascii_fold(text: str) -> str:
    """ø→o, æ→ae, å→a, then strip remaining diacritics."""
    t = text.replace("ø", "o").replace("æ", "ae").replace("å", "a")
    t = unicodedata.normalize("NFKD", t)
    return "".join(c for c in t if not unicodedata.combining(c))

def _slug(text: str) -> str:
    """Alphanumeric slug preserving æøå."""
    return re.sub(r"[^a-z0-9æøå]", "", _norm(text))

def _slug_ascii(text: str) -> str:
    """Alphanumeric slug with diacritics folded to ASCII."""
    return re.sub(r"[^a-z0-9]", "", _ascii_fold(_norm(text)))

def _name_tokens(name: str) -> List[str]:
    return [p for p in re.findall(r"[a-z0-9æøå]+", _norm(name)) if len(p) > 1]

def _extract_handle(url: str) -> str:
    if not isinstance(url, str):
        return ""
    m = re.search(r"tiktok\.com/@([^/?&#\s]+)", url.lower())
    return m.group(1) if m else ""

def _canonical_url(url: str) -> str:
    h = _extract_handle(url)
    return f"https://www.tiktok.com/@{h}" if h else url

def _is_tiktok(url: str) -> bool:
    return isinstance(url, str) and "tiktok.com/" in url.lower()


# ─────────────────────────────────────────────────────────────────────────────
# HANDLE GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def _handle_candidates(name: str, abbrev: str) -> List[str]:
    """Return likely TikTok handle variants for a name + party abbreviation."""
    tokens = _name_tokens(name)
    if not tokens:
        return []

    first, last = tokens[0], tokens[-1]
    af = _ascii_fold

    raw = []

    # Full name concatenated
    full = "".join(tokens)
    raw += [full, _slug_ascii(full)]

    # First + last (skip middle names)
    raw += [first + last, af(first) + af(last)]

    # Separated variants
    raw += [
        f"{first}.{last}",        f"{af(first)}.{af(last)}",
        f"{first}_{last}",        f"{af(first)}_{af(last)}",
    ]

    # With party abbreviation
    if abbrev:
        raw += [
            f"{first}{last}_{abbrev}",        f"{first}{last}{abbrev}",
            f"{af(first)}{af(last)}_{abbrev}", f"{af(first)}{af(last)}{abbrev}",
            f"{last}_{abbrev}",               f"{last}{abbrev}",
            f"{af(last)}_{abbrev}",           f"{af(last)}{abbrev}",
        ]

    # Middle-name initials (e.g. "lars c lilleholt" → "larscl")
    if len(tokens) >= 3:
        initials = "".join(t[0] for t in tokens[1:-1])
        raw += [f"{first}{initials}{last}", f"{af(first)}{initials}{af(last)}"]

    # Deduplicate, keep only TikTok-legal characters
    seen: set = set()
    result = []
    for g in raw:
        g = re.sub(r"[^a-z0-9._]", "", g)
        if g and g not in seen:
            seen.add(g)
            result.append(g)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# PARTY PATTERN FEEDBACK
# ─────────────────────────────────────────────────────────────────────────────

def _identify_handle_pattern(handle: str, name: str, abbrev: str) -> Optional[str]:
    """Map a confirmed handle back to the pattern that generated it."""
    tokens = _name_tokens(name)
    if not tokens:
        return None
    first, last = tokens[0], tokens[-1]
    af = _ascii_fold
    h  = re.sub(r"[^a-z0-9._]", "", handle.lower())

    candidates = [
        ("firstlast",        _slug_ascii("".join(tokens))),
        ("first_last",       f"{af(first)}_{af(last)}"),
        ("first.last",       f"{af(first)}.{af(last)}"),
        ("firstlast_abbrev", f"{af(first)}{af(last)}_{abbrev}" if abbrev else None),
        ("last_abbrev",      f"{af(last)}_{abbrev}"            if abbrev else None),
        ("lastabbrev",       f"{af(last)}{abbrev}"             if abbrev else None),
    ]
    for pattern_name, expected in candidates:
        if expected and (h == expected or h.startswith(expected)):
            return pattern_name
    return "other"

def _pattern_query(name: str, party_csv: str) -> Optional[str]:
    """
    If a party has a dominant confirmed handle pattern, return a targeted
    site:tiktok.com query based on it. Returns None until enough data exists.
    """
    canonical, _, abbrev = _lookup_party(party_csv)
    counter = _party_patterns.get(canonical)
    if not counter:
        return None
    top_pattern, top_count = counter.most_common(1)[0]
    if top_count < PATTERN_MIN_HITS:
        return None

    tokens = _name_tokens(name)
    if not tokens:
        return None
    first, last = tokens[0], tokens[-1]
    af = _ascii_fold

    pattern_map: Dict[str, Optional[str]] = {
        "firstlast":        _slug_ascii("".join(tokens)),
        "first_last":       f"{af(first)}_{af(last)}",
        "first.last":       f"{af(first)}.{af(last)}",
        "firstlast_abbrev": f"{af(first)}{af(last)}_{abbrev}" if abbrev else None,
        "last_abbrev":      f"{af(last)}_{abbrev}"            if abbrev else None,
        "lastabbrev":       f"{af(last)}{abbrev}"             if abbrev else None,
    }
    handle = pattern_map.get(top_pattern)
    if not handle:
        return None
    handle = re.sub(r"[^a-z0-9._]", "", handle)
    return f'site:tiktok.com "@{handle}"' if handle else None


# ─────────────────────────────────────────────────────────────────────────────
# QUERY BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_queries(name: str, party_csv: str) -> List[Tuple[str, str]]:
    """Return (query, engine) pairs in priority order."""
    canonical, _, abbrev = _lookup_party(party_csv)
    handles = _handle_candidates(name, abbrev)
    queries: List[Tuple[str, str]] = []

    # Party-pattern query first (only present once enough matches are confirmed)
    pattern_q = _pattern_query(name, party_csv)
    if pattern_q:
        queries.append((pattern_q, "ddg"))

    # Full-name site search
    queries.append((f'site:tiktok.com "{name}"', "ddg"))

    # Direct handle guesses (top 2)
    for h in handles[:2]:
        queries.append((f'site:tiktok.com "@{h}"', "ddg"))

    # Google (fired in parallel with the first DDG query)
    queries.append((f'"{name}" tiktok {canonical}', "google"))

    return queries


# ─────────────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────────────

def score_result(name: str, party_csv: str, title: str, snippet: str, url: str) -> dict:
    canonical, _, abbrev = _lookup_party(party_csv)
    handle       = _extract_handle(url)
    canon_url    = _canonical_url(url)
    handle_slug  = _slug(handle)
    handle_ascii = _slug_ascii(handle)
    searchable   = _norm(" ".join([title, snippet, canon_url, handle]))
    tokens       = _name_tokens(name)
    guesses      = _handle_candidates(name, abbrev)

    # 1. Handle match (0–55): decays slightly for lower-ranked guesses
    handle_score = 0
    if handle_slug or handle_ascii:
        for rank, guess in enumerate(guesses):
            guess_ascii = _slug_ascii(guess)
            if handle_slug == guess or handle_ascii == guess_ascii:
                handle_score = max(handle_score, 55 - rank * 2)
                break
            if (handle_ascii.startswith(guess_ascii) and len(guess_ascii) >= 4) \
               or (handle_slug.startswith(guess) and len(guess) >= 4):
                handle_score = max(handle_score, 35 - rank)
            elif tokens and _ascii_fold(tokens[-1]) in handle_ascii:
                handle_score = max(handle_score, 18)

    # 2. Name tokens in searchable text (0–25)
    if tokens:
        matched    = sum(1 for t in tokens if t in searchable or _ascii_fold(t) in searchable)
        name_score = int(25 * matched / len(tokens))
        if _slug_ascii(name) in re.sub(r"[^a-z0-9]", "", searchable):
            name_score = max(name_score, 25)
    else:
        name_score = 0

    # 3. Party mention (0–15)
    party_score = 15 if canonical in searchable else (8 if abbrev and abbrev in searchable else 0)

    # 4. Political context words (0–5)
    context_words = ["politiker", "kandidat", "folketing", "valgkreds", "stiller op"]
    context_score = 5 if any(w in searchable for w in context_words) else 0

    return {
        "profile_url":    canon_url,
        "profile_handle": handle,
        "handle_score":   handle_score,
        "name_score":     name_score,
        "party_score":    party_score,
        "context_score":  context_score,
        "total_score":    min(100, handle_score + name_score + party_score + context_score),
    }

def _classify(score: int) -> str:
    if score >= HIGH_CONFIDENCE: return "high_confidence"
    if score >= POSSIBLE_MATCH:  return "possible_match"
    if score > 0:                return "manual_review"
    return "no_match"


# ─────────────────────────────────────────────────────────────────────────────
# SEARCH ENGINES
# ─────────────────────────────────────────────────────────────────────────────

GOOGLE_URL = "https://www.google.com/search"

def _ddg_search_sync(query: str) -> List[dict]:
    """Blocking DDG search — called via run_in_executor to keep the event loop free."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            with DDGS() as ddgs:
                results = ddgs.text(query, max_results=MAX_RESULTS)
            return [
                {"href": r.get("href", ""), "title": r.get("title", ""), "body": r.get("body", "")}
                for r in (results or [])
            ]
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt + random.uniform(0, 1))
            else:
                return [{"error": str(e)}]
    return []

async def search_ddg(query: str) -> List[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _ddg_search_sync, query)

async def search_google(client: httpx.AsyncClient, query: str) -> List[dict]:
    params = {"q": query, "num": MAX_RESULTS, "hl": "da", "gl": "dk"}
    for attempt in range(MAX_RETRIES + 1):
        try:
            resp = await client.get(
                GOOGLE_URL, params=params,
                timeout=REQUEST_TIMEOUT, follow_redirects=True,
            )
            if resp.status_code == 429:
                raise httpx.HTTPStatusError("rate limited", request=resp.request, response=resp)
            resp.raise_for_status()
            break
        except Exception as e:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2 ** attempt + random.uniform(0, 1.5))
            else:
                return [{"error": str(e)}]

    results = []
    for m in re.finditer(r'href="/url\?q=(https?://[^&"]+)', resp.text):
        url = m.group(1)
        if "google.com" not in url and "googleadservices" not in url:
            results.append({"href": url, "title": "", "body": ""})
        if len(results) >= MAX_RESULTS:
            break
    return results


# ─────────────────────────────────────────────────────────────────────────────
# ADAPTIVE SLEEP
# ─────────────────────────────────────────────────────────────────────────────

def _record_ddg_success() -> None:
    global _ddg_errors, _ddg_successes, _sleep_min, _sleep_max
    _ddg_errors     = 0
    _ddg_successes += 1
    if _ddg_successes >= 10:
        _sleep_min = max(0.3, _sleep_min - 0.05)
        _sleep_max = max(0.7, _sleep_max - 0.05)
        _ddg_successes = 0

def _record_ddg_error() -> None:
    global _ddg_errors, _ddg_successes, _sleep_min, _sleep_max
    _ddg_errors   += 1
    _ddg_successes = 0
    if _ddg_errors >= 3:
        _sleep_min = min(3.0, _sleep_min + 0.3)
        _sleep_max = min(5.0, _sleep_max + 0.5)


# ─────────────────────────────────────────────────────────────────────────────
# TIKTOK PROFILE VERIFIER
# ─────────────────────────────────────────────────────────────────────────────

async def _verify_profile(client: httpx.AsyncClient, url: str) -> bool:
    """HEAD request to confirm the profile exists. Fails open on network error."""
    try:
        resp = await client.head(url, timeout=6, follow_redirects=True)
        return resp.status_code < 400
    except Exception:
        return True


# ─────────────────────────────────────────────────────────────────────────────
# PER-CANDIDATE SEARCH
# ─────────────────────────────────────────────────────────────────────────────

def _make_row(name: str, party: str, storkreds: str, query: str, engine: str,
              q_idx: int, r: dict, scores: dict, status: str) -> dict:
    return {
        "candidate_name": name,
        "party_name":     party,
        "storkreds":      storkreds,
        "query_used":     query,
        "search_engine":  engine,
        "query_phase":    q_idx + 1,
        "result_title":   r.get("title", ""),
        "result_snippet": r.get("body", "")[:200],
        **scores,
        "status": status,
    }

def _no_result_row(name: str, party: str, storkreds: str) -> dict:
    empty_scores = {
        "profile_url": "", "profile_handle": "",
        "handle_score": 0, "name_score": 0, "party_score": 0,
        "context_score": 0, "total_score": 0,
    }
    return _make_row(name, party, storkreds, "", "", -1, {}, empty_scores, "no_match")

def _error_row(name: str, party: str, storkreds: str,
               query: str, engine: str, error: str) -> dict:
    row = _no_result_row(name, party, storkreds)
    row.update({"query_used": query, "search_engine": engine,
                "result_snippet": f"ERROR: {error}", "status": "search_error"})
    return row

async def search_candidate(
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
    name: str, party: str, storkreds: str,
    use_google: bool,
) -> List[dict]:

    all_queries  = build_queries(name, party)
    ddg_queries  = [(i, q) for i, (q, e) in enumerate(all_queries) if e == "ddg"]
    google_queries = [(i, q) for i, (q, e) in enumerate(all_queries) if e == "google"]

    seen: set       = set()
    rows: List[dict] = []
    best_score      = -1
    google_tasks: List[asyncio.Task] = []

    async def _process(raw_results: List[dict], query: str, engine: str, q_idx: int) -> None:
        nonlocal best_score
        for r in raw_results:
            if "error" in r:
                rows.append(_error_row(name, party, storkreds, query, engine, r["error"]))
                if engine == "ddg":
                    _record_ddg_error()
                continue
            if engine == "ddg":
                _record_ddg_success()

            url = r.get("href", "")
            if not _is_tiktok(url):
                continue
            canon = _canonical_url(url)
            if canon in seen:
                continue
            seen.add(canon)

            scores = score_result(name, party, r.get("title", ""), r.get("body", ""), url)

            # Verify any promising profile is actually live
            if scores["total_score"] >= POSSIBLE_MATCH:
                if not await _verify_profile(client, scores["profile_url"]):
                    rows.append(_make_row(name, party, storkreds, query, engine, q_idx, r, scores, "dead_link"))
                    continue

            status = _classify(scores["total_score"])
            rows.append(_make_row(name, party, storkreds, query, engine, q_idx, r, scores, status))
            if scores["total_score"] > best_score:
                best_score = scores["total_score"]

    async with sem:
        for phase, (q_idx, query) in enumerate(ddg_queries):
            if best_score >= HIGH_CONFIDENCE:
                break

            # Fire all Google queries in the background on the first DDG phase
            if phase == 0 and use_google and google_queries:
                google_tasks = [
                    asyncio.create_task(search_google(client, gq))
                    for _, gq in google_queries
                ]

            ddg_results = await search_ddg(query)
            await asyncio.sleep(random.uniform(_sleep_min, _sleep_max))
            await _process(ddg_results, query, "ddg", q_idx)

            # Collect any Google results already ready (non-blocking)
            for (gi, gq), task in zip(google_queries, google_tasks):
                try:
                    g_results = await asyncio.wait_for(asyncio.shield(task), timeout=0.01)
                    await _process(g_results, gq, "google", gi)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

        # Collect any Google results still in flight
        for (gi, gq), task in zip(google_queries, google_tasks):
            if task.done():
                try:
                    await _process(task.result(), gq, "google", gi)
                except Exception:
                    pass
            else:
                try:
                    await _process(await asyncio.wait_for(task, timeout=REQUEST_TIMEOUT),
                                   gq, "google", gi)
                except Exception as e:
                    rows.append(_error_row(name, party, storkreds, gq, "google", str(e)))

    if not rows:
        rows.append(_no_result_row(name, party, storkreds))
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# PRIORITY SORT
# ─────────────────────────────────────────────────────────────────────────────

def prioritise(df: pd.DataFrame) -> pd.DataFrame:
    """Sort candidates: highest estimated TikTok presence first."""
    df = df.copy()
    df["_ppri"] = df["party_name"].apply(lambda p: _lookup_party(p)[1])
    df["_npri"] = df["candidate_name"].apply(lambda n: -len(n.split()))
    df = df.sort_values(["_ppri", "_npri"], ascending=[False, False])
    return df.drop(columns=["_ppri", "_npri"]).reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# CHECKPOINT
# ─────────────────────────────────────────────────────────────────────────────

def load_checkpoint(reset: bool) -> List[str]:
    if reset:
        CHECKPOINT_FILE.unlink(missing_ok=True)
        RAW_OUT.unlink(missing_ok=True)
        print("Reset: starting fresh.")
        return []
    if CHECKPOINT_FILE.exists():
        try:
            done = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
            print(f"Resuming: {len(done)} candidates already done.")
            return done
        except Exception as e:
            print(f"Checkpoint unreadable ({e}), starting fresh.")
    return []

def save_checkpoint(done_names: List[str]) -> None:
    CHECKPOINT_FILE.write_text(
        json.dumps(done_names, ensure_ascii=False), encoding="utf-8"
    )

def append_rows_to_csv(rows: List[dict]) -> None:
    if not rows:
        return
    cols         = list(rows[0].keys())
    write_header = not RAW_OUT.exists()
    with RAW_OUT.open("a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        if write_header:
            w.writeheader()
        w.writerows(rows)


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def save_outputs(original_df: pd.DataFrame) -> None:
    if not RAW_OUT.exists():
        print("No raw output found.")
        return

    raw_df  = pd.read_csv(RAW_OUT, encoding="utf-8-sig")
    raw_df  = raw_df.sort_values(["candidate_name", "total_score"], ascending=[True, False])

    best_df = raw_df.drop_duplicates(subset=["candidate_name"], keep="first").copy()
    best_df.to_csv(BEST_OUT, index=False, encoding="utf-8-sig")
    print(f"Best matches:  {BEST_OUT}  ({len(best_df)} rows)")

    low = best_df[best_df["total_score"] < POSSIBLE_MATCH]["candidate_name"]
    original_df[original_df["candidate_name"].isin(low)].to_csv(
        NO_MATCH_OUT, index=False, encoding="utf-8-sig"
    )
    print(f"No/low match:  {NO_MATCH_OUT}  ({len(low)} rows)")

    has_url = best_df[best_df["profile_url"].str.len() > 0]
    dups    = has_url[has_url.duplicated(subset=["profile_url"], keep=False)]
    if not dups.empty:
        dups.sort_values("profile_url").to_csv(DUPLICATE_OUT, index=False, encoding="utf-8-sig")
        print(f"Duplicate profiles: {DUPLICATE_OUT}  ({len(dups)} rows — needs review)")

    print("\nSummary:")
    for status, cnt in best_df["status"].value_counts().items():
        print(f"  {status:<20} {cnt:>4}")

    if _party_patterns:
        print("\nDominant handle patterns by party:")
        for party, counter in sorted(_party_patterns.items()):
            top = counter.most_common(1)
            if top:
                print(f"  {party:<28} {top[0][0]}  (n={top[0][1]})")


# ─────────────────────────────────────────────────────────────────────────────
# WORKER POOL
# ─────────────────────────────────────────────────────────────────────────────

async def _worker(
    candidate_iter: Iterator,
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    use_google: bool,
    done_names: List[str],
    lock: asyncio.Lock,
    pbar: atqdm,
    shutdown: asyncio.Event,
) -> int:
    completed = 0
    while not shutdown.is_set():
        async with lock:
            try:
                row = next(candidate_iter)
            except StopIteration:
                break

        rows = await search_candidate(
            sem, client,
            row.candidate_name, row.party_name, row.storkreds,
            use_google,
        )

        async with lock:
            append_rows_to_csv(rows)
            done_names.append(row.candidate_name)
            completed += 1
            pbar.update(1)

            if len(done_names) % CHECKPOINT_EVERY == 0:
                save_checkpoint(done_names)
                pbar.set_postfix_str(f"checkpoint @ {len(done_names)}")

            # Update party pattern feedback
            canonical, _, abbrev = _lookup_party(row.party_name)
            for r in rows:
                if r.get("total_score", 0) >= HIGH_CONFIDENCE and r.get("profile_handle"):
                    pattern = _identify_handle_pattern(r["profile_handle"], row.candidate_name, abbrev)
                    if pattern:
                        _party_patterns[canonical][pattern] += 1

    return completed


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    args = parse_args()

    df = pd.read_csv(args.input)
    required = {"candidate_name", "party_name", "storkreds"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df[list(required)].copy()
    df = df.dropna(subset=["candidate_name"])
    df["party_name"] = df["party_name"].fillna("")
    df["storkreds"]  = df["storkreds"].fillna("")
    df = prioritise(df)

    print(f"Loaded {len(df)} candidates.")

    done_names = load_checkpoint(args.reset)
    todo_df    = df[~df["candidate_name"].isin(set(done_names))]

    print(f"Remaining: {len(todo_df)}  |  Done: {len(done_names)}  |  "
          f"Google: {'off' if args.no_google else 'on'}\n")

    if todo_df.empty:
        print("All done — generating outputs.")
        save_outputs(df)
        CHECKPOINT_FILE.unlink(missing_ok=True)
        return

    shutdown = asyncio.Event()

    def _on_keyboard_interrupt(signum, frame):
        if not shutdown.is_set():
            print("\nStopping after current batch...")
            shutdown.set()

    import signal
    signal.signal(signal.SIGINT, _on_keyboard_interrupt)

    sem    = asyncio.Semaphore(args.concurrency)
    lock   = asyncio.Lock()
    limits = httpx.Limits(
        max_keepalive_connections=args.concurrency,
        max_connections=args.concurrency + 4,
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
    }

    start          = time.perf_counter()
    candidate_iter = iter(todo_df.itertuples(index=False))

    async with httpx.AsyncClient(limits=limits, headers=headers) as client:
        pbar = atqdm(total=len(todo_df), desc="Searching", unit="candidate")
        workers = [
            _worker(candidate_iter, client, sem, not args.no_google,
                    done_names, lock, pbar, shutdown)
            for _ in range(args.concurrency)
        ]
        results = await asyncio.gather(*workers)
        pbar.close()

    save_checkpoint(done_names)
    completed = sum(results)
    elapsed   = time.perf_counter() - start

    save_outputs(df)

    if shutdown.is_set():
        remaining = len(df) - len(done_names)
        print(f"\nStopped after {completed} this session. {remaining} remaining — run again to resume.")
    else:
        CHECKPOINT_FILE.unlink(missing_ok=True)
        print(f"\nFinished in {elapsed:.0f}s  (~{elapsed / max(completed, 1):.1f}s per candidate)")


if __name__ == "__main__":
    asyncio.run(main())