import re
import csv
from pathlib import Path

import pdfplumber


PDF_DIR = Path(r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\Candidate PDFs")
OUTPUT_CSV = Path(r"C:\Users\kaspe\Documents\GitHub\2026-Politician-Tiktok-Analysis\Candidate Dataset\candidates_list_2026.csv")

PARTIES = [
    "Socialdemokratiet",
    "Radikale Venstre",
    "Det Konservative Folkeparti",
    "SF - Socialistisk Folkeparti",
    "Borgernes Parti - Lars Boje Mathiesen",
    "Liberal Alliance",
    "Moderaterne",
    "Dansk Folkeparti",
    "Venstre",
    "Danmarksdemokraterne - Inger Støjberg",
    "Enhedslisten - De Rød-Grønne",
    "Alternativet",
    "Uden for partierne",
]

NOISE_PATTERNS = [
    r"^Folketingsvalg",
    r"^FORTEGNELSE OVER OPSTILLEDE$",
    r"^KANDIDATER I STORKREDSEN$",
    r"^Storkredsen omfatter følgende opstillingskredse og kommuner:?$",
    r"^Kandidaternes navne på stemmesedlen$",
    r"^Side \d+ af \d+$",
    r"^\d+\.\s",
]


def clean_text(s: str) -> str:
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def is_noise(line: str) -> bool:
    line = clean_text(line)
    if not line:
        return True
    for pat in NOISE_PATTERNS:
        if re.match(pat, line, flags=re.IGNORECASE):
            return True
    return False


def clean_candidate_name(line: str) -> str:
    line = clean_text(line)
    line = re.sub(r"\s+Alle$", "", line, flags=re.IGNORECASE).strip()
    line = re.sub(r"\s+\d+$", "", line).strip()
    return clean_text(line)


def looks_like_candidate(line: str) -> bool:
    line = clean_text(line)
    if is_noise(line):
        return False

    # Kandidatlinjer ender ofte på "Alle" eller et nummer
    if not re.search(r"(?:\sAlle|\s\d+)$", line, flags=re.IGNORECASE):
        return False

    name = clean_candidate_name(line)

    if len(name.split()) < 2:
        return False

    # Fjern linjer der tydeligvis ikke er navne
    banned = [
        "Storkredsen",
        "Folketingsvalg",
        "Opstillet i opstillingskreds",
        "Kandidaternes navne",
    ]
    if any(b.lower() in name.lower() for b in banned):
        return False

    # Rimelig navneform
    if not re.match(r"^[A-ZÆØÅ][A-Za-zÀ-ÿ0-9 .,'’\-()]+$", name):
        return False

    return True


def detect_party(line: str):
    line = clean_text(line)

    if "Opstillet i opstillingskreds" not in line and line not in PARTIES:
        # Vi tillader også rene partinavne
        pass

    for party in PARTIES:
        if party.lower() in line.lower():
            return party

    return None


def split_mixed_line(line: str):
    """
    Håndterer linjer der kan indeholde både kandidat og parti-header.
    Returnerer liste af segmenter i rækkefølge.
    """
    line = clean_text(line)

    hits = []
    for party in PARTIES:
        idx = line.lower().find(party.lower())
        if idx != -1:
            hits.append((idx, party))

    if not hits:
        return [line]

    idx, party = min(hits, key=lambda x: x[0])

    parts = []
    left = clean_text(line[:idx])
    right = clean_text(line[idx:])

    if left:
        parts.append(left)
    if right:
        parts.append(right)

    return parts


def extract_lines_from_page(page):
    """
    Bygger linjer fra ord-positioner i stedet for page.extract_text().
    Det er ofte mere robust på PDF'er med kolonner eller mærkelig tekststrøm.
    """
    words = page.extract_words(
        use_text_flow=False,
        keep_blank_chars=False,
        x_tolerance=2,
        y_tolerance=3,
    )

    if not words:
        return []

    # sorter top->venstre
    words = sorted(words, key=lambda w: (round(w["top"], 1), w["x0"]))

    lines = []
    current_words = []
    current_top = None
    threshold = 3.0

    for w in words:
        text = clean_text(w["text"])
        if not text:
            continue

        top = float(w["top"])

        if current_top is None:
            current_top = top
            current_words = [w]
            continue

        if abs(top - current_top) <= threshold:
            current_words.append(w)
        else:
            current_words = sorted(current_words, key=lambda x: x["x0"])
            line = " ".join(clean_text(x["text"]) for x in current_words if clean_text(x["text"]))
            line = clean_text(line)
            if line:
                lines.append(line)

            current_words = [w]
            current_top = top

    if current_words:
        current_words = sorted(current_words, key=lambda x: x["x0"])
        line = " ".join(clean_text(x["text"]) for x in current_words if clean_text(x["text"]))
        line = clean_text(line)
        if line:
            lines.append(line)

    return lines


def detect_storkreds(pdf_path: Path, pages) -> str:
    for page in pages[:2]:
        lines = extract_lines_from_page(page)
        for line in lines:
            if "Storkreds" in line:
                return line
    return pdf_path.stem


def parse_pdf(pdf_path: Path):
    rows = []

    with pdfplumber.open(pdf_path) as pdf:
        storkreds = detect_storkreds(pdf_path, pdf.pages)
        current_party = None

        print(f"\nParser: {pdf_path.name}")
        print(f"Storkreds: {storkreds}")

        for page_no, page in enumerate(pdf.pages, start=1):
            lines = extract_lines_from_page(page)

            for raw_line in lines:
                segments = split_mixed_line(raw_line)

                for line in segments:
                    line = clean_text(line)
                    if not line:
                        continue

                    party = detect_party(line)
                    if party:
                        current_party = party
                        continue

                    if current_party and looks_like_candidate(line):
                        rows.append({
                            "candidate_name": clean_candidate_name(line),
                            "party_name": current_party,
                            "storkreds": storkreds,
                        })

        print(f"Fundet i fil: {len(rows)}")

    return rows


def main():
    pdf_files = sorted(PDF_DIR.glob("*.pdf"))
    if not pdf_files:
        raise FileNotFoundError(f"Ingen PDF-filer fundet i {PDF_DIR.resolve()}")

    all_rows = []
    for pdf_path in pdf_files:
        try:
            all_rows.extend(parse_pdf(pdf_path))
        except Exception as e:
            print(f"Fejl i {pdf_path.name}: {e}")

    # fjern dubletter
    seen = set()
    unique_rows = []
    for row in all_rows:
        key = (row["candidate_name"], row["party_name"], row["storkreds"])
        if key not in seen:
            seen.add(key)
            unique_rows.append(row)

    unique_rows.sort(key=lambda x: (x["storkreds"], x["party_name"], x["candidate_name"]))

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["candidate_name", "party_name", "storkreds"])
        writer.writeheader()
        writer.writerows(unique_rows)

    print(f"\nFærdig. Gemt i: {OUTPUT_CSV.resolve()}")
    print(f"Antal rækker: {len(unique_rows)}")


if __name__ == "__main__":
    main()
