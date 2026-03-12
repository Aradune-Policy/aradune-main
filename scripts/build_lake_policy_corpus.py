#!/usr/bin/env python3
"""
Ingest CMS policy guidance (CIBs, SHO letters, SMD letters) into the Aradune data lake.

Phase 1: Scrape medicaid.gov/federal-policy-guidance listing pages,
download PDFs, extract text, chunk, and write Parquet.

Usage:
    python3 scripts/build_lake_policy_corpus.py                    # full run
    python3 scripts/build_lake_policy_corpus.py --skip-download    # just reprocess already-downloaded PDFs
    python3 scripts/build_lake_policy_corpus.py --max-pages 5      # limit to first 5 listing pages
    python3 scripts/build_lake_policy_corpus.py --dry-run          # show what would be done
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://www.medicaid.gov"
LISTING_URL = f"{BASE_URL}/federal-policy-guidance"
PDF_DIR = Path("data/raw/policy_guidance_pdfs")
LAKE_DIR = Path("data/lake")
METADATA_DIR = LAKE_DIR / "metadata"

CHUNK_SIZE = 800       # tokens (~600 words)
CHUNK_OVERLAP = 200    # tokens
MIN_CHUNK_SIZE = 100   # discard smaller fragments

# Approximate tokens per word
TOKENS_PER_WORD = 1.3

TODAY = date.today().isoformat()


def _curl(url: str, output_path: str | None = None, timeout: int = 30) -> str | None:
    """Fetch URL via curl. Returns content or saves to file."""
    cmd = ["curl", "-sL", "--max-time", str(timeout)]
    if output_path:
        cmd += ["-o", output_path]
    cmd.append(url)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 10)
        if output_path:
            return output_path if os.path.exists(output_path) else None
        return result.stdout
    except Exception as e:
        print(f"  curl error: {e}")
        return None


# ---------------------------------------------------------------------------
# Step 1: Scrape listing pages for document metadata
# ---------------------------------------------------------------------------

def scrape_listing_pages(max_pages: int = 200) -> list[dict]:
    """Scrape medicaid.gov federal policy guidance listing pages."""
    all_docs = []
    page = 0

    while page < max_pages:
        url = f"{LISTING_URL}?page={page}"
        print(f"  Fetching listing page {page + 1}...", end=" ", flush=True)

        html = _curl(url, timeout=30)
        if not html or len(html) < 1000:
            print("empty/error, stopping.")
            break

        # Parse document entries from HTML
        docs = _parse_listing_html(html)
        if not docs:
            print("no docs found, stopping.")
            break

        print(f"{len(docs)} docs")
        all_docs.extend(docs)

        # Check for next page — stop if current page is the last
        if f"page={page + 1}" not in html:
            print("  Last page reached.")
            break

        page += 1
        time.sleep(0.5)  # Be polite

    return all_docs


def _parse_listing_html(html: str) -> list[dict]:
    """Extract document entries from listing page HTML.

    medicaid.gov uses USWDS accordion components:
      <h4><button aria-controls="node-XXX">Title</button></h4>
      <div id="node-XXX">
        <b>Date:</b> Month DD, YYYY
        <a href="/federal-policy-guidance/downloads/xxx.pdf">Title</a>
      </div>
    """
    docs = []

    # Split HTML into accordion blocks using the node-XXXXX pattern
    blocks = re.split(r'(?=<h4[^>]*>\s*<button[^>]*aria-controls)', html)

    for block in blocks:
        if len(block) < 100:
            continue

        # Extract title from button text
        title_match = re.search(
            r'<button[^>]*>\s*(.*?)\s*</button>',
            block, re.DOTALL
        )
        if not title_match:
            continue
        title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
        title = re.sub(r'&\w+;', "'", title)  # &#039; → '
        title = re.sub(r'\s+', ' ', title)
        if not title or len(title) < 10:
            continue
        # Skip page furniture (handle smart quotes and HTML entities)
        title_check = title.lower().replace('\u2019', "'").replace('\u2018', "'")
        if title_check.startswith("here") and "know" in title_check[:30]:
            continue
        if title_check.startswith("an official"):
            continue

        # Extract date
        date_match = re.search(r'<b>Date:</b>\s*(\w+ \d{1,2},? \d{4})', block)
        pub_date = _parse_date(date_match.group(1)) if date_match else None

        # Extract PDF URL
        pdf_match = re.search(
            r'href="(/federal-policy-guidance/downloads/[^"]+\.pdf)"',
            block
        )
        pdf_url = BASE_URL + pdf_match.group(1) if pdf_match else None
        pdf_href = pdf_match.group(1) if pdf_match else ""

        if not pdf_url:
            continue  # Skip entries without PDF downloads

        # Determine doc type
        doc_type = _classify_doc_type(pdf_href, title)

        # Generate doc_id
        doc_id = _make_doc_id(doc_type, title, pub_date)

        docs.append({
            "doc_id": doc_id,
            "doc_type": doc_type,
            "title": title,
            "pdf_url": pdf_url,
            "source_url": BASE_URL + "/federal-policy-guidance",
            "publication_date": pub_date,
        })

    return docs


def _classify_doc_type(href: str, title: str) -> str:
    """Classify document as CIB, SHO, SMD, or other."""
    href_lower = href.lower()
    title_lower = title.lower()
    if "cib" in href_lower or "informational bulletin" in title_lower:
        return "cib"
    if "sho" in href_lower or "state health official" in title_lower:
        return "sho"
    if "smd" in href_lower or "state medicaid director" in title_lower:
        return "smd"
    return "guidance"


def _parse_date(date_str: str) -> str | None:
    """Parse a date string into ISO format."""
    if not date_str:
        return None
    for fmt in ["%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _make_doc_id(doc_type: str, title: str, pub_date: str | None) -> str:
    """Generate a stable doc_id from type + title hash."""
    slug = re.sub(r'[^a-z0-9]+', '-', title.lower())[:60].strip('-')
    h = hashlib.md5(title.encode()).hexdigest()[:8]
    prefix = doc_type
    if pub_date:
        prefix += f"-{pub_date}"
    return f"{prefix}-{slug}-{h}"


# ---------------------------------------------------------------------------
# Step 2: Download PDFs
# ---------------------------------------------------------------------------

def download_pdfs(docs: list[dict]) -> list[dict]:
    """Download PDFs for documents that have PDF URLs."""
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    skipped = 0

    for doc in docs:
        if not doc.get("pdf_url"):
            continue

        filename = doc["doc_id"] + ".pdf"
        filepath = PDF_DIR / filename
        doc["pdf_path"] = str(filepath)

        if filepath.exists() and filepath.stat().st_size > 1000:
            skipped += 1
            continue

        print(f"  Downloading: {doc['title'][:60]}...", end=" ", flush=True)
        result = _curl(doc["pdf_url"], str(filepath), timeout=60)
        if result and filepath.exists() and filepath.stat().st_size > 1000:
            downloaded += 1
            print("OK")
        else:
            print("FAILED")
            doc["pdf_path"] = None

        time.sleep(0.3)

    print(f"  Downloads: {downloaded} new, {skipped} cached")
    return docs


# ---------------------------------------------------------------------------
# Step 3: Extract text from PDFs
# ---------------------------------------------------------------------------

def extract_text(docs: list[dict]) -> list[dict]:
    """Extract text from downloaded PDFs using pdfplumber."""
    try:
        import pdfplumber
    except ImportError:
        print("  WARNING: pdfplumber not installed. Run: pip3 install pdfplumber")
        print("  Falling back to basic extraction...")
        return _extract_text_basic(docs)

    extracted = 0
    for doc in docs:
        if not doc.get("pdf_path") or not Path(doc["pdf_path"]).exists():
            doc["full_text"] = ""
            doc["page_count"] = 0
            continue

        try:
            with pdfplumber.open(doc["pdf_path"]) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    pages.append(text)

                doc["full_text"] = "\n\n".join(pages)
                doc["page_count"] = len(pages)
                extracted += 1
        except Exception as e:
            print(f"  Extract error ({doc['title'][:40]}): {e}")
            doc["full_text"] = ""
            doc["page_count"] = 0

    print(f"  Extracted text from {extracted} PDFs")
    return docs


def _extract_text_basic(docs: list[dict]) -> list[dict]:
    """Basic text extraction fallback without pdfplumber."""
    for doc in docs:
        doc["full_text"] = ""
        doc["page_count"] = 0
    return docs


# ---------------------------------------------------------------------------
# Step 4: Chunk documents
# ---------------------------------------------------------------------------

def chunk_documents(docs: list[dict]) -> tuple[list[dict], list[dict]]:
    """Chunk document text into overlapping windows. Returns (doc_records, chunk_records)."""
    doc_records = []
    chunk_records = []

    for doc in docs:
        text = doc.get("full_text", "")
        if not text or len(text) < 50:
            continue

        chunks = _section_aware_chunk(text)
        doc["chunk_count"] = len(chunks)

        # Extract state codes mentioned
        state_code = _extract_state_code(doc["title"], text)

        doc_records.append({
            "doc_id": doc["doc_id"],
            "doc_type": doc["doc_type"],
            "state_code": state_code,
            "title": doc["title"],
            "doc_number": _extract_doc_number(doc["title"]),
            "effective_date": None,
            "publication_date": doc.get("publication_date"),
            "status": "active",
            "source_url": doc.get("source_url", ""),
            "pdf_path": doc.get("pdf_path", ""),
            "page_count": doc.get("page_count", 0),
            "summary": text[:300].replace("\n", " ").strip() if text else None,
            "topics": _extract_topics(doc["title"], text),
            "chunk_count": len(chunks),
            "ingested_at": datetime.now().isoformat(),
        })

        for i, chunk in enumerate(chunks):
            chunk_id = f"{doc['doc_id']}-c{i:03d}"
            word_count = len(chunk["text"].split())
            token_count = int(word_count * TOKENS_PER_WORD)

            chunk_records.append({
                "chunk_id": chunk_id,
                "doc_id": doc["doc_id"],
                "doc_type": doc["doc_type"],
                "state_code": state_code,
                "chunk_index": i,
                "text": chunk["text"],
                "section_title": chunk.get("section_title"),
                "page_start": chunk.get("page_start"),
                "page_end": chunk.get("page_end"),
                "token_count": token_count,
            })

    return doc_records, chunk_records


def _section_aware_chunk(text: str) -> list[dict]:
    """Split text into chunks respecting section boundaries."""
    # Split into sections first (look for headers)
    section_pattern = re.compile(
        r'^(?:#{1,3}\s+|[A-Z][A-Z\s]{4,}$|(?:Section|Part|Title|Article)\s+\d+|'
        r'(?:I{1,3}V?|V?I{0,3})\.\s+[A-Z])',
        re.MULTILINE
    )

    sections = []
    matches = list(section_pattern.finditer(text))

    if matches:
        for i, match in enumerate(matches):
            start = match.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            section_title = match.group().strip()[:100]
            section_text = text[start:end].strip()
            if section_text:
                sections.append({"title": section_title, "text": section_text})
    else:
        # No sections found — treat as one block
        sections = [{"title": None, "text": text}]

    # Now chunk each section with sliding window
    chunks = []
    max_words = int(CHUNK_SIZE / TOKENS_PER_WORD)
    overlap_words = int(CHUNK_OVERLAP / TOKENS_PER_WORD)
    min_words = int(MIN_CHUNK_SIZE / TOKENS_PER_WORD)

    for section in sections:
        words = section["text"].split()
        if len(words) <= max_words:
            if len(words) >= min_words:
                chunks.append({
                    "text": section["text"],
                    "section_title": section["title"],
                })
            continue

        # Sliding window
        pos = 0
        while pos < len(words):
            end = min(pos + max_words, len(words))
            chunk_words = words[pos:end]

            # Try to break at sentence boundary
            chunk_text = " ".join(chunk_words)
            last_period = max(
                chunk_text.rfind(". "),
                chunk_text.rfind(".\n"),
                chunk_text.rfind("? "),
                chunk_text.rfind("! "),
            )
            if last_period > len(chunk_text) * 0.5:
                chunk_text = chunk_text[:last_period + 1]

            if len(chunk_text.split()) >= min_words:
                chunks.append({
                    "text": chunk_text.strip(),
                    "section_title": section["title"],
                })

            pos += max_words - overlap_words
            if end >= len(words):
                break

    return chunks


def _is_bad_title(title: str) -> bool:
    """Check if a title is page furniture, not a real document title."""
    t = title.lower().replace('\u2019', "'").replace('\u2018', "'")
    if "here" in t[:10] and "know" in t[:30]:
        return True
    if t.startswith("an official"):
        return True
    if t.startswith("department of health"):
        return True
    return False


def _extract_title_from_text(text: str) -> str | None:
    """Extract a meaningful title from PDF text content.

    CMS documents typically have patterns like:
        RE: Subject Line Here
        Subject: Topic Here
        Dear State ... \n\n First substantive paragraph
    """
    if not text:
        return None
    lines = text[:3000].split("\n")

    # Look for RE: or Subject: line
    for line in lines:
        line = line.strip()
        m = re.match(r'(?:RE|Re|Subject|SUBJECT):\s*(.+)', line)
        if m and len(m.group(1).strip()) > 15:
            return m.group(1).strip()[:200]

    # Look for a substantive line after "Dear" greeting
    past_dear = False
    for line in lines:
        line = line.strip()
        if line.lower().startswith("dear "):
            past_dear = True
            continue
        if past_dear and len(line) > 30 and not line.startswith("7500") and not line.startswith("Center"):
            return line[:200]

    # Fallback: first line that looks like a title (>20 chars, <200, not boilerplate)
    skip = {"department of health", "centers for medicare", "7500 security", "baltimore", "cms.gov"}
    for line in lines:
        line = line.strip()
        if len(line) > 20 and len(line) < 200 and not any(s in line.lower() for s in skip):
            return line

    return None


def _extract_state_code(title: str, text: str) -> str | None:
    """Try to extract a state code from the title."""
    # Most CIBs/SHOs are federal — no state code
    return None


def _extract_doc_number(title: str) -> str | None:
    """Extract document number like CIB-01232026 or SHO 25-005."""
    m = re.search(r'(?:CIB|SHO|SMD)\s*[-#]?\s*(\d[\d-]+)', title, re.IGNORECASE)
    if m:
        return m.group(0).strip()
    m = re.search(r'\(([A-Z]{3}\s*\d[\d-]+)\)', title)
    if m:
        return m.group(1).strip()
    return None


def _extract_topics(title: str, text: str) -> str | None:
    """Extract topic tags from title keywords."""
    topics = []
    kw_map = {
        "enrollment": ["enrollment", "eligibility", "redetermination"],
        "rates": ["rate", "payment", "reimbursement", "fee schedule"],
        "managed_care": ["managed care", "mco", "capitation"],
        "pharmacy": ["drug", "pharmacy", "prescription", "nadac"],
        "hcbs": ["hcbs", "home and community", "waiver", "1915"],
        "behavioral_health": ["behavioral", "mental health", "substance", "opioid"],
        "maternal_health": ["maternal", "pregnancy", "postpartum"],
        "chip": ["chip", "children's health"],
        "quality": ["quality", "core set", "measure"],
        "unwinding": ["unwinding", "continuous eligibility", "phe"],
        "1115": ["1115", "demonstration", "waiver"],
        "fmap": ["fmap", "federal medical assistance"],
        "ltss": ["long-term", "nursing", "institutional"],
    }
    title_lower = title.lower()
    text_preview = text[:2000].lower()
    for topic, keywords in kw_map.items():
        if any(kw in title_lower or kw in text_preview for kw in keywords):
            topics.append(topic)
    return ",".join(topics) if topics else None


# ---------------------------------------------------------------------------
# Step 5: Write Parquet
# ---------------------------------------------------------------------------

def write_parquet(doc_records: list[dict], chunk_records: list[dict], dry_run: bool = False):
    """Write document and chunk records to Parquet files."""
    if dry_run:
        print(f"\n  [DRY RUN] Would write {len(doc_records)} documents, {len(chunk_records)} chunks")
        return

    import pandas as pd
    conn = duckdb.connect()

    # Write policy_document
    if doc_records:
        doc_path = LAKE_DIR / "fact" / "policy_document" / f"snapshot={TODAY}"
        doc_path.mkdir(parents=True, exist_ok=True)
        out = doc_path / "data.parquet"

        df = pd.DataFrame(doc_records)
        conn.execute("CREATE TABLE docs AS SELECT * FROM df")
        conn.execute(f"""
            COPY docs TO '{out}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """)
        print(f"  Wrote {len(doc_records)} documents → {out} ({out.stat().st_size / 1024:.1f} KB)")

    # Write policy_chunk
    if chunk_records:
        chunk_path = LAKE_DIR / "fact" / "policy_chunk" / f"snapshot={TODAY}"
        chunk_path.mkdir(parents=True, exist_ok=True)
        out = chunk_path / "data.parquet"

        df = pd.DataFrame(chunk_records)
        conn.execute("CREATE TABLE chunks AS SELECT * FROM df")
        conn.execute(f"""
            COPY chunks TO '{out}'
            (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """)
        print(f"  Wrote {len(chunk_records)} chunks → {out} ({out.stat().st_size / 1024:.1f} KB)")

    # Write manifest
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "table": "policy_corpus",
        "snapshot_date": TODAY,
        "documents": len(doc_records),
        "chunks": len(chunk_records),
        "doc_types": {},
        "source": "medicaid.gov/federal-policy-guidance",
    }
    for d in doc_records:
        t = d["doc_type"]
        manifest["doc_types"][t] = manifest["doc_types"].get(t, 0) + 1

    manifest_path = METADATA_DIR / f"manifest_policy_corpus_{TODAY}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest → {manifest_path}")

    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest CMS policy guidance into data lake")
    parser.add_argument("--max-pages", type=int, default=200, help="Max listing pages to scrape")
    parser.add_argument("--skip-download", action="store_true", help="Skip PDF download, reprocess existing")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()

    print("=" * 60)
    print("Policy Corpus Ingestion — Phase 1: CIBs + SHO Letters")
    print("=" * 60)

    # Step 1: Scrape listing pages
    print("\n[1/5] Scraping document listing...")
    cache_file = PDF_DIR / "_doc_metadata.json"

    if args.skip_download and cache_file.exists():
        docs = json.loads(cache_file.read_text())
        print(f"  Loaded {len(docs)} docs from cache")
    else:
        docs = scrape_listing_pages(max_pages=args.max_pages)
        print(f"  Found {len(docs)} documents total")

        # Filter to CIBs, SHOs, SMDs (skip regulations, proposed rules)
        docs = [d for d in docs if d["doc_type"] in ("cib", "sho", "smd", "guidance")]
        docs = [d for d in docs if d.get("pdf_url")]
        print(f"  {len(docs)} with downloadable PDFs")

        # Cache metadata
        PDF_DIR.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(json.dumps(docs, indent=2, default=str))

    if args.dry_run:
        print(f"\n  [DRY RUN] Would process {len(docs)} documents")
        for d in docs[:10]:
            print(f"    {d['doc_type'].upper():8s} {d.get('publication_date', 'no date'):12s} {d['title'][:60]}")
        if len(docs) > 10:
            print(f"    ... and {len(docs) - 10} more")
        return

    # Step 2: Download PDFs
    print(f"\n[2/5] Downloading PDFs...")
    if not args.skip_download:
        docs = download_pdfs(docs)
    else:
        # Set pdf_path from existing files
        for doc in docs:
            filepath = PDF_DIR / (doc["doc_id"] + ".pdf")
            doc["pdf_path"] = str(filepath) if filepath.exists() else None

    # Step 3: Extract text
    print(f"\n[3/5] Extracting text from PDFs...")
    docs = extract_text(docs)
    docs_with_text = [d for d in docs if d.get("full_text") and len(d["full_text"]) > 50]
    print(f"  {len(docs_with_text)} documents with extractable text")

    # Step 3b: Fix bad titles from PDF content
    print(f"\n[3b/5] Fixing document titles...")
    fixed = 0
    for doc in docs_with_text:
        if _is_bad_title(doc.get("title", "")):
            better = _extract_title_from_text(doc.get("full_text", ""))
            if better:
                doc["title"] = better
                doc["doc_id"] = _make_doc_id(doc["doc_type"], better, doc.get("publication_date"))
                fixed += 1
    print(f"  Fixed {fixed} titles from PDF content")

    # Step 4: Chunk
    print(f"\n[4/5] Chunking documents...")
    doc_records, chunk_records = chunk_documents(docs_with_text)
    print(f"  {len(doc_records)} documents → {len(chunk_records)} chunks")
    if chunk_records:
        avg_tokens = sum(c["token_count"] for c in chunk_records) / len(chunk_records)
        print(f"  Average chunk: {avg_tokens:.0f} tokens")

    # Step 5: Write Parquet
    print(f"\n[5/5] Writing Parquet...")
    write_parquet(doc_records, chunk_records)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Done! {len(doc_records)} documents, {len(chunk_records)} chunks")
    by_type = {}
    for d in doc_records:
        by_type[d["doc_type"]] = by_type.get(d["doc_type"], 0) + 1
    for t, c in sorted(by_type.items()):
        print(f"  {t.upper():10s} {c:4d} documents")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
