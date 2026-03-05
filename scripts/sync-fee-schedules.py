#!/usr/bin/env python3
"""
sync-fee-schedules.py
Syncs fee schedule data from mfs_scraper/aradune.db into the frontend's JSON files.
Run after refresh.py or any DB update.

Outputs:
  public/data/medicaid_rates.json   — merged fee schedule rates (best-of DB + existing)
  public/data/conversion_factors.json — state conversion factors + methodology
  public/data/fee_schedule_rates.json — code-centric view for the Rate Lookup tool
"""
import sqlite3
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "mfs_scraper" / "aradune.db"
DATA_DIR = ROOT / "public" / "data"

def load_db():
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)
    return sqlite3.connect(str(DB_PATH))

def build_medicaid_rates(db):
    """Merge DB fee schedule rates with existing medicaid_rates.json.
    For each state, keep whichever source has more codes."""
    existing_path = DATA_DIR / "medicaid_rates.json"
    existing = {}
    if existing_path.exists():
        with open(existing_path) as f:
            existing = json.load(f)

    # Pull DB rates: base rates only (no modifier or empty modifier), deduplicated
    cur = db.execute("""
        SELECT state_code, procedure_code, rate, description
        FROM rates
        WHERE rate > 0 AND (modifier = '' OR modifier IS NULL)
        GROUP BY state_code, procedure_code
        HAVING rate = MAX(rate)
        ORDER BY state_code, procedure_code
    """)

    db_rates = {}
    for row in cur:
        st, code, rate, desc = row
        if st not in db_rates:
            db_rates[st] = {}
        db_rates[st][code] = [round(rate, 2), desc or "", "fee_schedule"]

    # Merge: for each state, use whichever has more codes
    merged = {}
    all_states = sorted(set(list(existing.keys()) + list(db_rates.keys())))

    for st in all_states:
        ex = existing.get(st, {})
        db = db_rates.get(st, {})

        if len(db) >= len(ex) * 0.9 and len(db) > 0:
            # DB has comparable or better coverage — use DB, backfill from existing
            merged[st] = dict(db)
            for code, val in ex.items():
                if code not in merged[st]:
                    merged[st][code] = val
        else:
            # Existing has better coverage — use existing, backfill from DB
            merged[st] = dict(ex)
            for code, val in db.items():
                if code not in merged[st]:
                    merged[st][code] = val

    # Stats
    total_codes = sum(len(v) for v in merged.values())
    new_states = sorted(set(db_rates.keys()) - set(existing.keys()))
    print(f"medicaid_rates.json: {len(merged)} states, {total_codes:,} codes")
    if new_states:
        print(f"  New states from DB: {', '.join(new_states)}")

    return merged

def build_conversion_factors(db):
    """Export conversion factors and methodology from the states table.
    Output schema per state:
      conversion_factors: [{name, value}]   -- named CFs (practitioner, lab, anesthesia, etc.)
      cf_notes: str                         -- caveats (guardrails, multipliers, etc.)
      methodology, methodology_detail, update_frequency, gpci_approach, fee_schedule_type
    """
    cur = db.execute("""
        SELECT state_code, state_name, methodology, methodology_detail,
               conversion_factor, anesthesia_cf, update_frequency,
               gpci_approach, fee_schedule_type
        FROM states
        ORDER BY state_code
    """)
    factors = {}
    for row in cur:
        st = row[0]
        factors[st] = {
            "name": row[1],
            "methodology": row[2] or "",
            "methodology_detail": row[3] or "",
            "conversion_factors": [],
            "cf_notes": "",
            "update_frequency": row[6] or "",
            "gpci_approach": row[7] or "",
            "fee_schedule_type": row[8] or "",
        }

    # ── Verified CFs only ───────────────────────────────────────────────
    # Only include conversion factors sourced from SPAs, fee setting manuals,
    # or production scripts. DB-estimated CFs are unreliable and omitted.
    # Add states here as they are verified against primary sources.
    VERIFIED_CFS = {
        "FL": {
            "cfs": [
                {"name": "Practitioner", "value": 24.9880},
                {"name": "Lab", "value": 26.1689},
                {"name": "Anesthesia", "value": 14.15},
            ],
            "notes": (
                "Category-specific CFs per SPA FL-24-0002. Rates also subject to "
                "+/-10% annual guardrail, FSI multiplier (1.04), and FCSO locale weighting."
            ),
        },
        "KY": {
            "cfs": [
                {"name": "Practitioner", "value": 29.67},
                {"name": "Anesthesia", "value": 15.20},
            ],
            "notes": "Codified in 907 KAR 3:010 (eff. March 2023). Anesthesia: (procedure RVU + time units) x $15.20.",
        },
        "NV": {
            "cfs": [
                {"name": "Practitioner", "value": 35.8228},
            ],
            "notes": "Frozen at the CY2014 Medicare CF per NV-11-005. Payment = (Work RVU x GPCI + PE RVU x GPCI + MP RVU x GPCI) x $35.8228.",
        },
    }
    for st, v in VERIFIED_CFS.items():
        if st in factors:
            factors[st]["conversion_factors"] = v["cfs"]
            factors[st]["cf_notes"] = v["notes"]

    with_cf = sum(1 for v in factors.values() if v["conversion_factors"])
    total_cfs = sum(len(v["conversion_factors"]) for v in factors.values())
    print(f"conversion_factors.json: {len(factors)} states, {with_cf} verified CFs ({total_cfs} total)")
    return factors

def build_fee_schedule_rates(db):
    """Code-centric view: for each code, all states' rates + Medicare rate.
    Used by the Rate Lookup tool."""
    # Get Medicare rates
    medicare = {}
    try:
        with open(DATA_DIR / "medicare_rates.json") as f:
            med_data = json.load(f)
            for code, entry in med_data.get("rates", {}).items():
                if isinstance(entry, dict) and entry.get("r"):
                    medicare[code] = {
                        "rate": entry["r"],
                        "desc": entry.get("d", ""),
                        "rvu": entry.get("rvu"),
                    }
    except FileNotFoundError:
        print("  Warning: medicare_rates.json not found, skipping Medicare data")

    # Get all rates from DB, base modifier only
    cur = db.execute("""
        SELECT procedure_code, state_code, rate, description
        FROM rates
        WHERE rate > 0 AND (modifier = '' OR modifier IS NULL)
        GROUP BY state_code, procedure_code
        HAVING rate = MAX(rate)
        ORDER BY procedure_code, state_code
    """)

    codes = {}
    for row in cur:
        code, st, rate, desc = row
        if code not in codes:
            codes[code] = {"states": {}, "desc": desc or ""}
        codes[code]["states"][st] = round(rate, 2)
        if desc and not codes[code]["desc"]:
            codes[code]["desc"] = desc

    # Enrich with Medicare
    for code in codes:
        if code in medicare:
            codes[code]["medicare"] = medicare[code]["rate"]
            if not codes[code]["desc"] and medicare[code]["desc"]:
                codes[code]["desc"] = medicare[code]["desc"]

    # Filter to codes with at least 3 states (useful for comparison)
    useful = {k: v for k, v in codes.items() if len(v["states"]) >= 3}
    print(f"fee_schedule_rates.json: {len(useful):,} codes (3+ states), {len(codes):,} total")
    return useful

def main():
    print("Syncing fee schedule data from aradune.db → frontend JSON...\n")
    db = load_db()

    # 1. Medicaid rates
    rates = build_medicaid_rates(db)
    out = DATA_DIR / "medicaid_rates.json"
    with open(out, "w") as f:
        json.dump(rates, f, separators=(",", ":"))
    print(f"  → {out} ({os.path.getsize(out) / 1e6:.1f} MB)\n")

    # 2. Conversion factors
    factors = build_conversion_factors(db)
    out = DATA_DIR / "conversion_factors.json"
    with open(out, "w") as f:
        json.dump(factors, f, separators=(",", ":"), indent=None)
    print(f"  → {out} ({os.path.getsize(out) / 1e3:.0f} KB)\n")

    # 3. Fee schedule rates (code-centric)
    fs_rates = build_fee_schedule_rates(db)
    out = DATA_DIR / "fee_schedule_rates.json"
    with open(out, "w") as f:
        json.dump(fs_rates, f, separators=(",", ":"))
    print(f"  → {out} ({os.path.getsize(out) / 1e6:.1f} MB)\n")

    db.close()
    print("Done.")

if __name__ == "__main__":
    main()
