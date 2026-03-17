#!/usr/bin/env python3
"""
ETL: Extract structured indicator data from ~300 MCPAR PDFs.

Extracts:
  - Section B, Topic I:  BI.1 (statewide enrollment), BI.2 (MC enrollment)
  - Section C, Topic I:  C1I.3 (program type), C1I.5 (program enrollment)
  - Section B, Topic X:  BX.2 (overpayment standard), BX.7a-c (provider termination)
  - Section D, Topic IV: D1IV.1 (appeals resolved), D1IV.10 (grievances resolved)
  - Section A:           A5a/A5b (reporting period), A6 (program name)

Output: data/lake/fact/mcpar_detail/data.parquet
"""

import os
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

# ── State abbreviation lookup ──────────────────────────────────────────────
STATE_ABBREVS = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    "Puerto Rico": "PR", "Guam": "GU", "US Virgin Islands": "VI",
    "American Samoa": "AS", "Northern Mariana Islands": "MP",
}

BASE = Path("/Users/jamestori/Desktop/Aradune/manual-data/mcpar")
YEAR_FOLDERS = {
    "mcpars-reports-by-performance-year2023-august2024": 2023,
    "mcpars-reports-by-performance-year2024": 2024,
    "mcpars-reports-by-performance-year2025": 2025,
}
OUT = Path("/Users/jamestori/Desktop/Aradune/data/lake/fact/mcpar_detail")


def parse_number(s: str) -> float | None:
    """Parse a number string like '1,296,845' or '0' into a float."""
    if not s:
        return None
    s = s.strip().replace(",", "").replace("$", "").replace("%", "")
    try:
        return float(s)
    except ValueError:
        return None


def extract_text(pdf_path: str) -> str:
    """Run pdftotext -layout on a PDF, return text or empty string."""
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", pdf_path, "-"],
            capture_output=True, text=True, timeout=30
        )
        return result.stdout if result.returncode == 0 else ""
    except (subprocess.TimeoutExpired, Exception):
        return ""


def extract_state_from_filename(filename: str) -> tuple[str, str]:
    """Extract state name and abbreviation from filename like 'Alabama_ Program_MCPAR_2023.pdf'."""
    # Pattern: "State_ Program_MCPAR_YEAR.pdf"
    m = re.match(r"^([^_]+)_\s*", filename)
    if m:
        state_name = m.group(1).strip()
        state_code = STATE_ABBREVS.get(state_name, "")
        return state_name, state_code
    return "", ""


def extract_program_from_filename(filename: str) -> str:
    """Extract program name from filename."""
    # Pattern: "State_ Program Name_MCPAR_YEAR.pdf" or "State_ Program Name_YEAR.pdf"
    m = re.match(r"^[^_]+_\s*(.+?)_(?:MCPAR_)?\d{4}", filename)
    if m:
        return m.group(1).strip().rstrip("_")
    return ""


def find_value_after_indicator(text: str, indicator_pattern: str, n_lines: int = 20) -> str:
    """Find lines matching indicator_pattern, then look in the next n_lines for the response value."""
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(indicator_pattern, line, re.IGNORECASE):
            # The response is typically on the same line (after wide spacing) or subsequent lines
            # Check same line first — look for content after large whitespace gap
            parts = re.split(r"\s{4,}", line.strip())
            if len(parts) >= 3:
                # Number  Indicator  Response format
                return parts[-1].strip()
            elif len(parts) == 2:
                return parts[-1].strip()
            # Check subsequent lines for the response
            for j in range(i + 1, min(i + n_lines, len(lines))):
                candidate = lines[j].strip()
                # Skip empty lines and lines that are part of the indicator description
                if not candidate:
                    continue
                # Skip lines that look like continued description (lowercase start, long text)
                if candidate[0].islower() and len(candidate) > 30:
                    continue
                # Skip common description fragments
                if any(kw in candidate.lower() for kw in [
                    "enter the", "provide the", "per 42 cfr", "states must",
                    "include all", "auto-populated", "numerator:", "denominator:",
                    "what is the", "select one", "respond with", "cms receives",
                ]):
                    continue
                return candidate
    return ""


def extract_enrollment_near(text: str, indicator_id: str) -> float | None:
    """Extract a number appearing near an indicator like BI.1 or C1I.5."""
    lines = text.split("\n")
    for i, line in enumerate(lines):
        # Match patterns like "BI.1", " BI.1 ", "B1.1" etc.
        if re.search(rf"\b{re.escape(indicator_id)}\b", line, re.IGNORECASE):
            # Look for a number on same line or next few lines
            # Same line: check for number after wide space
            nums = re.findall(r"[\d,]{3,}", line)
            if nums:
                val = parse_number(nums[-1])
                if val and val >= 100:
                    return val
            # Look in next 15 lines for a standalone number
            for j in range(i + 1, min(i + 15, len(lines))):
                candidate = lines[j].strip()
                # Look for a line that is just a number (possibly with commas)
                if re.match(r"^[\d,]+$", candidate):
                    val = parse_number(candidate)
                    if val is not None:
                        return val
                # Or a number on a line with other text but clearly a value
                nums = re.findall(r"([\d,]{4,})", candidate)
                if nums:
                    # Check it's not a date or phone number
                    for n in nums:
                        val = parse_number(n)
                        if val and val >= 100 and len(n.replace(",", "")) <= 10:
                            return val
    return None


def extract_program_type(text: str) -> str:
    """Extract program type from C1I.3 section."""
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(r"\bC1I\.3\b", line, re.IGNORECASE):
            # Check same line for the response
            parts = re.split(r"\s{4,}", line.strip())
            for p in parts:
                p = p.strip()
                if re.search(r"\b(MCO|PIHP|PAHP|PCCM)\b", p, re.IGNORECASE):
                    return p
            # Check next lines
            for j in range(i + 1, min(i + 20, len(lines))):
                candidate = lines[j].strip()
                if re.search(r"\b(MCO|PIHP|PAHP|PCCM)\b", candidate, re.IGNORECASE):
                    return candidate
                if re.search(r"managed care organization", candidate, re.IGNORECASE):
                    return candidate
                if re.search(r"prepaid", candidate, re.IGNORECASE):
                    return candidate
    return ""


def classify_program_type(raw: str) -> str:
    """Classify raw program type text into standard categories."""
    if not raw:
        return ""
    raw_lower = raw.lower()
    if "mco" in raw_lower or "managed care organization" in raw_lower:
        return "MCO"
    elif "pihp" in raw_lower or "prepaid inpatient" in raw_lower:
        return "PIHP"
    elif "pahp" in raw_lower or "prepaid ambulatory" in raw_lower:
        return "PAHP"
    elif "pccm" in raw_lower or "primary care case management" in raw_lower:
        return "PCCM"
    return raw.strip()[:80]


def extract_overpayment_standard(text: str) -> str:
    """Extract BX.2 overpayment contract standard."""
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(r"\bBX\.2\b", line, re.IGNORECASE):
            # Look for response on same line or next lines
            parts = re.split(r"\s{4,}", line.strip())
            for p in parts:
                p_lower = p.strip().lower()
                if any(kw in p_lower for kw in ["hybrid", "return", "retain", "state has"]):
                    return p.strip()
            for j in range(i + 1, min(i + 15, len(lines))):
                candidate = lines[j].strip()
                c_lower = candidate.lower()
                if any(kw in c_lower for kw in ["hybrid", "return", "retain", "state has"]):
                    return candidate
    return ""


def classify_overpayment(raw: str) -> str:
    """Classify overpayment standard into categories."""
    if not raw:
        return ""
    raw_lower = raw.lower()
    if "hybrid" in raw_lower:
        return "hybrid"
    elif "return" in raw_lower and "retain" not in raw_lower:
        return "return"
    elif "retain" in raw_lower and "return" not in raw_lower:
        return "retain"
    elif "hybrid" not in raw_lower and "return" in raw_lower and "retain" in raw_lower:
        return "hybrid"
    return raw[:60]


def extract_yes_no(text: str, indicator_id: str) -> str:
    """Extract Yes/No response for an indicator like BX.7a."""
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(rf"\b{re.escape(indicator_id)}\b", line, re.IGNORECASE):
            # Check same line
            parts = re.split(r"\s{4,}", line.strip())
            for p in parts:
                p_strip = p.strip()
                if p_strip.lower() in ("yes", "no", "n/a"):
                    return p_strip
            # Check next few lines
            for j in range(i + 1, min(i + 10, len(lines))):
                candidate = lines[j].strip()
                if candidate.lower() in ("yes", "no", "n/a"):
                    return candidate
    return ""


def sum_plan_level_numbers(text: str, indicator_id: str) -> float | None:
    """
    For plan-level indicators (D1IV.1, D1IV.10), sum all the numbers
    across all plans. These appear as:
      D1IV.1  Appeals resolved (...)    PlanName
                                        Number
                                        PlanName2
                                        Number2
    """
    lines = text.split("\n")
    total = 0.0
    found = False
    in_section = False
    blank_count = 0

    for i, line in enumerate(lines):
        if re.search(rf"\b{re.escape(indicator_id)}\b", line, re.IGNORECASE):
            in_section = True
            blank_count = 0
            # Check same line for a number
            nums = re.findall(r"\b([\d,]+)\b", line)
            for n in nums:
                val = parse_number(n)
                if val is not None and val >= 0:
                    # Skip small numbers that might be indicator numbers
                    if len(n.replace(",", "")) >= 1:
                        pass  # Will handle below
            continue

        if in_section:
            stripped = line.strip()
            if not stripped:
                blank_count += 1
                if blank_count > 5:
                    # Likely end of section
                    break
                continue

            # Check if we hit the next indicator
            if re.match(r"^D1IV\.\d|^Topic|^Section|^Number\s+Indicator", stripped):
                break

            # Check if this line is a number
            if re.match(r"^[\d,]+$", stripped):
                val = parse_number(stripped)
                if val is not None:
                    total += val
                    found = True
                blank_count = 0

            # Check for number at end of line (after plan name)
            else:
                nums = re.findall(r"\b([\d,]{1,})\s*$", stripped)
                if nums:
                    val = parse_number(nums[-1])
                    if val is not None and val > 0:
                        total += val
                        found = True
                blank_count = 0

    return total if found else None


def extract_reporting_period(text: str) -> tuple[str, str]:
    """Extract reporting period start/end dates from A5a/A5b."""
    start = ""
    end = ""
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(r"\bA5a\b", line, re.IGNORECASE) or re.search(r"Reporting period start", line, re.IGNORECASE):
            # Find date pattern
            for j in range(max(0, i), min(i + 8, len(lines))):
                m = re.search(r"(\d{2}/\d{2}/\d{4})", lines[j])
                if m:
                    start = m.group(1)
                    break
        if re.search(r"\bA5b\b", line, re.IGNORECASE) or re.search(r"Reporting period end", line, re.IGNORECASE):
            for j in range(max(0, i), min(i + 8, len(lines))):
                m = re.search(r"(\d{2}/\d{2}/\d{4})", lines[j])
                if m:
                    end = m.group(1)
                    break
    return start, end


def extract_special_benefits(text: str) -> str:
    """Extract C1I.4a special benefits."""
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if re.search(r"\bC1I\.4a\b", line, re.IGNORECASE):
            benefits = []
            # Check same line and next lines
            for j in range(i, min(i + 25, len(lines))):
                candidate = lines[j].strip()
                for kw in ["behavioral health", "long-term", "dental", "transportation", "none of the above"]:
                    if kw in candidate.lower():
                        benefits.append(kw.title())
            if benefits:
                return "; ".join(sorted(set(benefits)))
    return ""


def process_pdf(pdf_path: str, perf_year: int) -> dict:
    """Process a single PDF and return extracted data."""
    filename = os.path.basename(pdf_path)
    state_name, state_code = extract_state_from_filename(filename)
    program_name = extract_program_from_filename(filename)

    record = {
        "state_code": state_code,
        "state_name": state_name,
        "program_name": program_name,
        "performance_year": perf_year,
        "statewide_enrollment": None,
        "mc_enrollment": None,
        "program_enrollment": None,
        "program_type": "",
        "program_type_raw": "",
        "overpayment_standard": "",
        "overpayment_standard_raw": "",
        "provider_term_monitoring_7a": "",
        "provider_term_monitoring_7b": "",
        "provider_term_describe_7c": "",
        "appeals_resolved": None,
        "grievances_resolved": None,
        "special_benefits": "",
        "reporting_period_start": "",
        "reporting_period_end": "",
        "source_file": filename,
        "snapshot_date": str(date.today()),
    }

    text = extract_text(pdf_path)
    if not text:
        record["_error"] = "pdftotext failed"
        return record

    # ── Section B, Topic I ─────────────────────────────────────────────
    record["statewide_enrollment"] = extract_enrollment_near(text, "BI.1")
    # Also try B1.1 variant
    if record["statewide_enrollment"] is None:
        record["statewide_enrollment"] = extract_enrollment_near(text, "B1.1")

    record["mc_enrollment"] = extract_enrollment_near(text, "BI.2")
    if record["mc_enrollment"] is None:
        record["mc_enrollment"] = extract_enrollment_near(text, "B1.2")

    # ── Section C, Topic I ─────────────────────────────────────────────
    raw_type = extract_program_type(text)
    record["program_type_raw"] = raw_type
    record["program_type"] = classify_program_type(raw_type)

    record["program_enrollment"] = extract_enrollment_near(text, "C1I.5")

    # ── Section B, Topic X ─────────────────────────────────────────────
    raw_overpay = extract_overpayment_standard(text)
    record["overpayment_standard_raw"] = raw_overpay
    record["overpayment_standard"] = classify_overpayment(raw_overpay)

    record["provider_term_monitoring_7a"] = extract_yes_no(text, "BX.7a")
    record["provider_term_monitoring_7b"] = extract_yes_no(text, "BX.7b")

    # BX.7c is a description, grab first line
    bx7c_val = find_value_after_indicator(text, r"\bBX\.7c\b", 10)
    record["provider_term_describe_7c"] = bx7c_val[:200] if bx7c_val else ""

    # ── Section D, Topic IV ────────────────────────────────────────────
    record["appeals_resolved"] = sum_plan_level_numbers(text, "D1IV.1")
    # Also check if there's no D-section but a C-section count
    if record["appeals_resolved"] is None:
        record["appeals_resolved"] = sum_plan_level_numbers(text, "D1IV.1a")

    record["grievances_resolved"] = sum_plan_level_numbers(text, "D1IV.10")

    # ── Reporting period ───────────────────────────────────────────────
    rp_start, rp_end = extract_reporting_period(text)
    record["reporting_period_start"] = rp_start
    record["reporting_period_end"] = rp_end

    # ── Special benefits ───────────────────────────────────────────────
    record["special_benefits"] = extract_special_benefits(text)

    return record


def main():
    records = []
    total_pdfs = 0
    errors = 0

    for folder_name, perf_year in YEAR_FOLDERS.items():
        folder = BASE / folder_name
        if not folder.exists():
            print(f"WARNING: Folder not found: {folder}")
            continue

        pdfs = sorted(f for f in os.listdir(folder) if f.lower().endswith(".pdf"))
        print(f"\n{'='*60}")
        print(f"Processing {len(pdfs)} PDFs from {folder_name} (PY{perf_year})")
        print(f"{'='*60}")

        for idx, pdf_file in enumerate(pdfs, 1):
            pdf_path = str(folder / pdf_file)
            total_pdfs += 1

            record = process_pdf(pdf_path, perf_year)

            if record.get("_error"):
                errors += 1
                print(f"  [{idx:3d}/{len(pdfs)}] ERROR: {pdf_file[:60]} — {record['_error']}")
                del record["_error"]
            else:
                # Quick status
                sw = record["statewide_enrollment"]
                pe = record["program_enrollment"]
                pt = record["program_type"]
                print(f"  [{idx:3d}/{len(pdfs)}] {record['state_code']:2s} | {record['program_name'][:40]:40s} | SW={sw or '?':>12} | PE={pe or '?':>12} | Type={pt or '?'}")

            records.append(record)

    # ── Write to parquet via DuckDB ────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Total: {total_pdfs} PDFs processed, {errors} errors")
    print(f"Records: {len(records)}")

    if not records:
        print("No records to write!")
        sys.exit(1)

    import duckdb
    con = duckdb.connect()

    # Create table from records
    con.execute("CREATE TABLE mcpar_detail AS SELECT * FROM (VALUES " +
                "('','','',0,0.0,0.0,0.0,'','','','','','','','',0.0,0.0,'','','','')) " +
                "AS t(state_code,state_name,program_name,performance_year," +
                "statewide_enrollment,mc_enrollment,program_enrollment," +
                "program_type,program_type_raw,overpayment_standard,overpayment_standard_raw," +
                "provider_term_monitoring_7a,provider_term_monitoring_7b,provider_term_describe_7c," +
                "special_benefits,appeals_resolved,grievances_resolved," +
                "reporting_period_start,reporting_period_end,source_file,snapshot_date) " +
                "WHERE false")

    # Better approach: use Python list -> DuckDB
    con.execute("DROP TABLE mcpar_detail")

    # Build columns with proper types
    import pandas as pd
    df = pd.DataFrame(records)

    # Ensure proper types
    for col in ["statewide_enrollment", "mc_enrollment", "program_enrollment",
                "appeals_resolved", "grievances_resolved"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["performance_year"] = df["performance_year"].astype(int)

    # Clean up _error column if it somehow survived
    if "_error" in df.columns:
        df = df.drop(columns=["_error"])

    # Order columns
    col_order = [
        "state_code", "state_name", "program_name", "performance_year",
        "reporting_period_start", "reporting_period_end",
        "statewide_enrollment", "mc_enrollment", "program_enrollment",
        "program_type", "program_type_raw",
        "overpayment_standard", "overpayment_standard_raw",
        "provider_term_monitoring_7a", "provider_term_monitoring_7b",
        "provider_term_describe_7c",
        "appeals_resolved", "grievances_resolved",
        "special_benefits",
        "source_file", "snapshot_date",
    ]
    df = df[[c for c in col_order if c in df.columns]]

    # Register and write
    con.register("df_view", df)

    OUT.mkdir(parents=True, exist_ok=True)
    out_path = str(OUT / "data.parquet")
    con.execute(f"COPY (SELECT * FROM df_view ORDER BY state_code, performance_year, program_name) TO '{out_path}' (FORMAT PARQUET)")

    # Verify
    result = con.execute(f"SELECT COUNT(*) AS cnt, COUNT(DISTINCT state_code) AS n_states, COUNT(DISTINCT performance_year) AS n_years FROM read_parquet('{out_path}')").fetchone()
    print(f"\nWritten: {out_path}")
    print(f"  Rows: {result[0]}, States: {result[1]}, Years: {result[2]}")

    # Summary stats
    print("\n── Coverage Summary ──")
    summary = con.execute(f"""
        SELECT
            performance_year,
            COUNT(*) as programs,
            COUNT(DISTINCT state_code) as states,
            COUNT(statewide_enrollment) as has_sw_enroll,
            COUNT(program_enrollment) as has_prog_enroll,
            COUNT(NULLIF(program_type, '')) as has_prog_type,
            COUNT(NULLIF(overpayment_standard, '')) as has_overpay,
            COUNT(appeals_resolved) as has_appeals,
            COUNT(grievances_resolved) as has_grievances
        FROM read_parquet('{out_path}')
        GROUP BY performance_year
        ORDER BY performance_year
    """).fetchdf()
    print(summary.to_string(index=False))

    # Top enrollments
    print("\n── Top 10 Programs by Enrollment ──")
    top = con.execute(f"""
        SELECT state_code, performance_year, program_name, program_type,
               program_enrollment, statewide_enrollment
        FROM read_parquet('{out_path}')
        WHERE program_enrollment IS NOT NULL
        ORDER BY program_enrollment DESC
        LIMIT 10
    """).fetchdf()
    print(top.to_string(index=False))

    # Program type distribution
    print("\n── Program Type Distribution ──")
    types = con.execute(f"""
        SELECT program_type, COUNT(*) as cnt
        FROM read_parquet('{out_path}')
        WHERE program_type != ''
        GROUP BY program_type
        ORDER BY cnt DESC
    """).fetchdf()
    print(types.to_string(index=False))

    # Overpayment standard distribution
    print("\n── Overpayment Standard Distribution ──")
    overpay = con.execute(f"""
        SELECT overpayment_standard, COUNT(*) as cnt
        FROM read_parquet('{out_path}')
        WHERE overpayment_standard != ''
        GROUP BY overpayment_standard
        ORDER BY cnt DESC
    """).fetchdf()
    print(overpay.to_string(index=False))

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
