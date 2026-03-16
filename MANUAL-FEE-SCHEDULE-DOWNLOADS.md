# Fee Schedule Manual Downloads — Remaining States

> 7 states still need manual downloads. I already scraped/parsed IL and MT automatically.
> These require browser access due to form submissions, portals, or WAF blocks.

---

## 1. North Dakota (CSV — Easiest)

**Coverage gap**: 17% → should be 100%

1. Visit: https://www.hhs.nd.gov/healthcare/medicaid/provider/fee-schedules
2. Download the **physician fee schedule CSV** (updated weekly, refreshed Sundays)
3. Save to: `manual-data/fee-schedules/north-dakota/`

Format: Clean CSV. Should be trivially parseable.

---

## 2. Oregon (CSV/Excel available)

**Coverage gap**: 1% → should be 100%

1. Visit: https://www.oregon.gov/oha/hsd/ohp/pages/fee-schedule.aspx
2. Look for **"Medical/Dental FFS Rates"** section
3. Download the **CSV or Excel** file (NOT the 2-page PDF summary I already got)
4. Also grab the file specs doc: https://www.oregon.gov/oha/HSD/OHP/Tools/ffs-medical-dental-file-specs.pdf
5. Save to: `manual-data/fee-schedules/oregon/`

The Excel/CSV should have full HCPCS codes with rates. Oregon uses RBRVS at ~80% of Medicare.

---

## 3. California (Excel — needs form click)

**Coverage gap**: 8% → should be 100%

1. Visit: https://files.medi-cal.ca.gov/rates/rates_download.aspx
2. **Accept the AMA disclaimer** (checkbox + submit)
3. Select **"Physician/Practitioner"** or browse available fee schedule categories
4. Download the **Excel or text (zipped)** file
5. Save to: `manual-data/fee-schedules/california/`

The download includes both rates AND conversion factors in the same ZIP. Medi-Cal now pays ≥87.5% of Medicare.

---

## 4. Washington (Excel — navigate to category)

**Coverage gap**: 14% → should be 100%

1. Visit: https://www.hca.wa.gov/billers-providers-partners/prior-authorization-claims-and-billing/provider-billing-guides-and-fee-schedules
2. Find **"Physician-related/professional services"** section
3. Look for the most recent effective date (January 1, 2026 to present)
4. Download the **Excel** file
5. Save to: `manual-data/fee-schedules/washington/`

WA uses RBRVS. The Excel files contain computed rates. Updated quarterly.

---

## 5. North Carolina (ServiceNow portal)

**Coverage gap**: 33% → should be 100%

1. Visit: https://ncdhhs.servicenowservices.com/fee_schedules
2. Browse available fee schedule categories
3. Download the **physician/professional** fee schedule (Excel/CSV)
4. Also try: https://medicaid.ncdhhs.gov/document-collection/fee-schedules
5. Save to: `manual-data/fee-schedules/north-carolina/`

NC uses RBRVS. An 8% rate cut was applied then reversed in late 2025. Current rates should reflect the reversal.

---

## 6. Nebraska (PDF — may need parsing)

**Coverage gap**: 28% → should be higher

1. Visit: https://dhhs.ne.gov/Pages/Medicaid-Provider-Rates-and-Fee-Schedules.aspx
2. Download the **Physician** fee schedule
3. Check if Excel is available (some categories have Excel, others are PDF-only)
4. The latest should be January 2026
5. Save to: `manual-data/fee-schedules/nebraska/`

If PDF-only, I can parse it the same way I did NJ (773 pages) and MT.

---

## 7. Ohio (Portal navigation)

**Coverage gap**: 35% → should be 100%

1. Visit: https://medicaid.ohio.gov/resources-for-providers/billing/fee-schedule-and-rates/
2. OR try: https://ohpnm.omes.maximus.com → click **Fee Schedule** tab (no login needed)
3. Download the physician/professional fee schedule
4. Save to: `manual-data/fee-schedules/ohio/`

Ohio uses RBRVS with a 5%+ increase applied January 2024. The portal may have multiple redirect steps.

---

## Already Done (no action needed)

- **Vermont**: Computed from published CFs ($35.99 PC / $28.71 std) — 9,483 codes. Optional: download actual Excel from `dvha.vermont.gov/providers/codesfee-schedules` (blocks automated access, need browser)
- **Illinois**: Scraped automatically — 10,552 codes with E&M
- **Montana**: Scraped automatically — 9,047 codes with E&M
- **Pennsylvania**: 2,301 codes scraped. E&M codes behind PROMISe portal (needs provider login at `promise.pa.gov`). Low priority since PA has 42 E&M codes from the original CPRA scraper.

---

## Priority Order

1. **ND** (1 min — CSV, trivial)
2. **OR** (2 min — CSV/Excel on landing page)
3. **CA** (3 min — accept disclaimer, download zip)
4. **WA** (3 min — navigate to physician section)
5. **NC** (5 min — ServiceNow portal)
6. **OH** (5 min — portal navigation)
7. **NE** (5 min — may be PDF)

Total time: ~25 minutes for all 7.

After you download, drop the files in `manual-data/fee-schedules/{state}/` and let me know. I'll parse and ingest them all.
