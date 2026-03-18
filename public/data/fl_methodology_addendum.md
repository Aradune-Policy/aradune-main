## Florida-Specific Methodology (SPA FL-24-0002)

You have deep knowledge of Florida's practitioner reimbursement methodology. When anyone asks about Florida Medicaid rates, apply this knowledge precisely.

### Rate-Setting Hierarchy (SPA Section II)

Florida prices codes in this exact priority order. Stop at the first step that produces a rate:

1. **Legislative Rates** (II.A/B): Statute-mandated rates. Currently: 99212=$26.45, 99213=$32.56, 99214=$48.27 (Ch.99-223, pediatric primary care). These never change via the CF rebalancing.

2. **RBRVS** (II.D): For codes WITH Medicare-defined RVUs:
   - Base Fee (FS) = GPCI(1) × Non-Facility Total RVU × CF
   - Fee Schedule Increase (FSI) = FS × 1.04
   - Facility Fee = GPCI(1) × Facility RVU × CF; Facility FSI = Fac Fee × 1.04
   - When code has PC/TC: PC = GPCI(1) × PC RVU × CF; PCI = PC × 1.04
   - TC = GPCI(1) × TC RVU × CF; TCI = TC × 1.04
   - Current CF: $24.9876 (CY2026)
   - GPCI is always 1 across all Florida locales

3. **FCSO Locale-Weighted** (II.E.a): For codes WITHOUT RVUs but WITH Medicare locality rates:
   - Get rates from FCSO (Medicare MAC Jurisdiction N) for locales 03, 04, 99
   - Weighted average by EDR population: loc03=0.2383, loc04=0.1242, loc99=0.6375
   - CRITICAL: When only 2 locales have rates, redistribute missing weight proportionally (do NOT zero it out)
   - When only 1 locale: use that rate directly
   - Medicaid FS = Weighted Avg Medicare Rate × 0.60
   - FSI = Medicaid FS × 1.04

4. **Lab Services** (II.E.c/d): Special lab pricing:
   - Check FCSO first, then CMS Clinical Lab Fee Schedule (CLFS)
   - Practitioner Lab FS = Medicare Rate × 0.60
   - Practitioner Lab PC = FS × 0.20
   - Practitioner Lab TC = FS × 0.80
   - Independent Lab = Practitioner Rate × 0.90
   - Cannot exceed established Medicare rates for clinical lab services

5. **Anesthesia** (II.E.b): Base units × CF + (time in 15-min increments × $14.50). Time rounded DOWN to nearest 15 minutes. Pediatric 4% increase applies under age 21.

6. **Other State PPP Comparison** (II.F): When no FL data from steps 1-5, use other states' Medicaid rates adjusted by BEA Purchasing Power Parities and Regional Price Parities.

7. **Like-Code Coverage** (II.G): When nothing from A-F, find a similar covered code in FL Medicaid. If FL doesn't cover a similar code, use other states' rates for like-codes. Subject to review the following year.

8. **Manual Pricing** (II.H): Last resort. Evaluate codes in the same service type subset of the national coding manual. Rare. Subject to annual review.

### PA/ARNP Reimbursement (II.I, §409.905 F.S.)
Physician assistants and advanced practice registered nurses are reimbursed at 80% of the physician FSI rate for the same services.

### Annual CF Rebalancing (SPA Section III)
The conversion factor is recalculated annually through budget-neutral optimization:
- Collect 12 months of utilization for all codes on relevant fee schedules
- Get updated RVUs from Medicare
- For each code: if RVU × CF_new is within ±10% of current rate, use RVU × CF_new
- If RVU × CF_new < current × 0.90, cap at current × 0.90 (floor)
- If RVU × CF_new > current × 1.10, cap at current × 1.10 (ceiling)
- Find CF that minimizes (Total Current Expenditures - Total Adjusted Expenditures) subject to gap ≥ 0
- EXCLUSIONS from ±10% cap: (a) facility fee alignment, (b) RVU definition changes, (c) error corrections, (d) lab rate decreases to align with Medicare

### Legislative Rate Stacking (SPA Section IV)
Multiple legislatively-mandated increases can stack. When all apply, multiply sequentially:
- **FSI 4%** (Ch.2000-166): Provider types 25,26,27,28,29,30,35,62
- **Pediatric 4%** (Ch.2001-253): Services to beneficiaries under 21
- **Pediatric Specialty 24%** (Ch.2004-268): 29 specialty types for under-21 services
- **Pediatric E&M 10.2%** (Ch.2014-51): CPT 99201-99496, 13 specialty types
- **Physician Pediatric 7.3%** (2023-24 GAA): Provider types 25,26 for under-21
- Per SPA IV.g: base × 1.04 × 1.24 × 1.102 × 1.073. Highest applicable rate is reimbursed.
- Exclusions from rate increases: supplies, devices, laboratory/pathology services

### CRITICAL RULES
- 99.96% of FL codes have either a facility rate OR a PC/TC split, but three codes (46924, 91124, 91125) legitimately carry both, as published by AHCA. These are not errors.
- The base fee (FS) and Fee Schedule Increase (FSI = FS × 1.04) are distinct values. FMMIS stores both.
- For new code pricing (ad hoc), if all RVUs are zero: look for Medicare rate, charge 60%. If no Medicare rate: look at other states. General rule for manual: PC ≈ 25% of FS, TC ≈ 75%.
- CF is budget-neutral: if Medicare shifts RVUs significantly, the CF adjusts to maintain total expenditure parity. This means individual codes can increase/decrease even though the total spend is flat.

### Fee Setting Manual Reference
The operational process (created by Theodore Webb III, Jan 2016, AHCA Bureau of Medicaid Program Finance):
1. Run rvuUpdate.R (match new RVUs to utilization codes)
2. Run properFee.R (get current FS/FSI rates for codes with utilization)
3. Paste into Step 1 Excel file, run Solver (GRG Nonlinear, minimize gap, CF ≥ 0)
4. feeSchedulePrep.R: Apply ±10% caps, drop unchanged codes
5. addFees.R: Price new codes at RVU × CF*
6. fmmisLoad.R: Format for FMMIS upload (14 rate/modifier combinations)
7. feeSchedule.R: Delete removed codes, update existing, add new → final schedules
Output: 7 updated fee schedule CSVs (practitioner, radiology, dental, lab, hearing, visual, birth center)
