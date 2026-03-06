###############################################################################
# Aradune · T-MSIS Sample Data Generator
#
# Purpose: Generate realistic synthetic test data matching the exact HHS
#          Medicaid Provider Spending schema so you can test the pipeline
#          without downloading 3.4GB first.
#
# Actual schema (from opendata.hhs.gov / HuggingFace mirror):
#   spending (227M rows):
#     BILLING_PROVIDER_NPI_NUM  (string)  - 10-digit NPI
#     SERVICING_PROVIDER_NPI_NUM (string) - 10-digit NPI
#     HCPCS_CODE                (string)  - procedure code
#     CLAIM_FROM_MONTH          (string)  - "YYYY-MM" format
#     TOTAL_UNIQUE_BENEFICIARIES (int)    - beneficiary count
#     TOTAL_CLAIMS              (int)     - claim count
#     TOTAL_PAID                (float)   - total $ paid by Medicaid
#
#   billing_providers (618K rows):
#     NPI, name, address, taxonomy, etc. from NPPES
#
#   servicing_providers (1.63M rows):
#     Same structure as billing_providers
#
#   hcpcs_codes (7.55K rows):
#     Code lookups with descriptions
#
# IMPORTANT DISCOVERY: The spending table has NO state column!
# State must be derived from provider tables (NPPES join on NPI).
# This is critical for the pipeline design.
#
# Usage: Rscript tmsis_sample_generator.R
# Output: sample_spending.csv, sample_providers.csv, sample_hcpcs.csv
###############################################################################

library(data.table)

cat("═══════════════════════════════════════════════\n")
cat("  Aradune · T-MSIS Sample Data Generator\n")
cat("═══════════════════════════════════════════════\n\n")

set.seed(42)
OUTPUT_DIR <- "."

# ── Configuration ──────────────────────────────────────────────────────────
N_PROVIDERS   <- 200     # Unique NPIs
N_ROWS        <- 5000    # Total spending rows (NPI × HCPCS × month combos)

# State distribution (weighted toward larger Medicaid programs)
STATES <- c("FL","NY","TX","CA","PA","OH","IL","GA","MN","AZ",
            "MA","MI","NC","WA","CO","NJ","MD","VA","IN","OR",
            "KY","LA","TN","MO","AL","SC","WI","CT","OK","AR",
            "MS","NV","IA","KS","NE","NM","WV","UT","ME","NH",
            "HI","ID","RI","MT","DE","SD","ND","AK","VT","WY","DC")
STATE_WEIGHTS <- c(12,18,14,22,8,7,7,5,4,5,
                   5,6,6,4,3,5,3,4,4,3,
                   3,3,3,3,2,2,3,2,2,2,
                   1,2,2,1,1,2,1,1,1,1,
                   1,1,1,1,1,0.5,0.5,0.5,0.5,0.3,1)

# HCPCS codes with realistic rate ranges and categories
CODES <- data.table(
  code = c(
    # E&M
    "99213","99214","99215","99211","99212",
    # HCBS
    "T1019","T2025","T1020","S5125","S5130",
    # Behavioral Health
    "97153","90834","90837","H0015","H2019",
    # Dental
    "D0120","D1110","D0150","D2392","D7140",
    # Maternity
    "59400","59510","59025",
    # Imaging
    "70553","71046","73721",
    # Surgery
    "46924","27447",
    # Drugs
    "J3490","J1100","J0585",
    # FL methodology flags
    "91124","91125"
  ),
  desc = c(
    "Office/outpatient visit est, low","Office/outpatient visit est, mod",
    "Office/outpatient visit est, high","Office/outpatient visit est, min",
    "Office/outpatient visit est, straightforward",
    "Personal care services per 15 min","Waiver services NOS",
    "Personal care services per diem","Attendant care services NOS",
    "Homemaker services NOS",
    "Adaptive behavior treatment","Psychotherapy 45 min",
    "Psychotherapy 60 min","Alcohol/drug services intensive OP",
    "Therapeutic behavioral services",
    "Periodic oral evaluation","Adult prophylaxis",
    "Comprehensive oral evaluation","Resin composite 2 surfaces",
    "Extraction erupted tooth/root",
    "Obstetric care total vaginal","Obstetric care total cesarean",
    "Fetal non-stress test",
    "Brain MRI w/wo contrast","Chest X-ray 2 views",
    "MRI knee w/wo contrast",
    "Destruction of hemorrhoids","Total knee replacement",
    "Unclassified drugs","Dexamethasone injection",
    "Botulinum toxin type A",
    "Esophageal motility study","Esophageal impedance test"
  ),
  cat = c(
    rep("E&M",5), rep("HCBS",5), rep("Behavioral",5),
    rep("Dental",5), rep("Maternity",3), rep("Imaging",3),
    rep("Surgery",2), rep("Drugs",3), rep("Diagnostic",2)
  ),
  # Average rate (used to generate realistic TOTAL_PAID)
  avg_rate = c(
    48,75,106,25,38,
    5.80,42,85,18,22,
    28,83,120,168,45,
    32,59,48,140,95,
    2150,2800,55,
    286,28,380,
    246,8500,
    185,12,450,
    188,165
  ),
  # Relative frequency weight (how often this code appears)
  freq_weight = c(
    20,15,5,8,12,
    25,10,3,4,3,
    8,10,5,3,4,
    12,8,6,4,5,
    2,1.5,3,
    2,8,2,
    0.5,0.3,
    6,4,1.5,
    0.3,0.2
  )
)

# Generate month range: Jan 2018 - Dec 2024
MONTHS <- as.character(seq(as.Date("2018-01-01"), as.Date("2024-12-01"), by = "month"))
MONTHS <- format(as.Date(MONTHS), "%Y-%m")

cat("Configuration:\n")
cat("  Providers:", N_PROVIDERS, "\n")
cat("  Spending rows:", N_ROWS, "\n")
cat("  HCPCS codes:", nrow(CODES), "\n")
cat("  Months:", length(MONTHS), "(", MONTHS[1], "to", MONTHS[length(MONTHS)], ")\n")
cat("  States:", length(STATES), "\n\n")

# ── Step 1: Generate Provider Table ────────────────────────────────────────
cat("Step 1: Generating providers...\n")

# Generate realistic 10-digit NPIs
gen_npi <- function(n) {
  sprintf("%010d", sample(1000000000:1999999999, n, replace = FALSE))
}

providers <- data.table(
  NPI = gen_npi(N_PROVIDERS),
  STATE = sample(STATES, N_PROVIDERS, replace = TRUE, prob = STATE_WEIGHTS),
  ENTITY_TYPE = sample(c("Individual","Organization"), N_PROVIDERS,
                       replace = TRUE, prob = c(0.7, 0.3)),
  TAXONOMY = sample(c(
    "207Q00000X",  # Family Medicine
    "208D00000X",  # General Practice
    "363L00000X",  # Nurse Practitioner
    "332B00000X",  # DME Supplier
    "174400000X",  # Personal Care Attendant
    "251E00000X",  # Home Health
    "261QM0801X",  # Mental Health
    "1223G0001X",  # General Practice Dentist
    "207V00000X",  # OB/GYN
    "2085R0202X"   # Diagnostic Radiology
  ), N_PROVIDERS, replace = TRUE,
  prob = c(15,10,12,5,15,10,10,8,5,5))
)

# Add provider names (synthetic)
first_names <- c("James","Maria","Robert","Linda","Michael","Patricia",
                 "David","Jennifer","William","Elizabeth","Richard","Susan",
                 "Joseph","Jessica","Thomas","Sarah","Charles","Karen","Daniel","Lisa")
last_names <- c("Smith","Johnson","Williams","Brown","Jones","Garcia","Miller",
                "Davis","Rodriguez","Martinez","Wilson","Anderson","Thomas",
                "Taylor","Moore","Jackson","Martin","Lee","Perez","Harris")
org_names <- c("Community Health","Caring Hands","Premier Care","Family Health",
               "Allied Services","Home Care Plus","Wellness Group","Metro Health",
               "Sunrise Care","Guardian Health")
org_suffixes <- c("Inc","LLC","Corp","Services","Associates","Partners","Group")

providers[ENTITY_TYPE == "Individual",
          PROVIDER_NAME := paste(
            sample(first_names, .N, replace = TRUE),
            sample(last_names, .N, replace = TRUE)
          )]
providers[ENTITY_TYPE == "Organization",
          PROVIDER_NAME := paste(
            sample(org_names, .N, replace = TRUE),
            sample(org_suffixes, .N, replace = TRUE)
          )]

cat("  Generated", N_PROVIDERS, "providers across", uniqueN(providers$STATE), "states\n")
cat("  Entity types:", providers[, .N, by = ENTITY_TYPE][, paste(ENTITY_TYPE, N, sep = "=", collapse = ", ")], "\n")

# ── Step 2: Generate Spending Table ────────────────────────────────────────
cat("\nStep 2: Generating spending records...\n")

# Each row = one provider × one HCPCS × one month
# Not every provider bills every code every month

spending <- data.table(
  BILLING_PROVIDER_NPI_NUM = character(0),
  SERVICING_PROVIDER_NPI_NUM = character(0),
  HCPCS_CODE = character(0),
  CLAIM_FROM_MONTH = character(0),
  TOTAL_UNIQUE_BENEFICIARIES = integer(0),
  TOTAL_CLAIMS = integer(0),
  TOTAL_PAID = numeric(0)
)

# Generate rows
npis <- sample(providers$NPI, N_ROWS, replace = TRUE)
codes <- sample(CODES$code, N_ROWS, replace = TRUE, prob = CODES$freq_weight)
months <- sample(MONTHS, N_ROWS, replace = TRUE)

# Look up rates and add realistic variation
rates <- CODES$avg_rate[match(codes, CODES$code)]
states <- providers$STATE[match(npis, providers$NPI)]

# State-level rate multiplier (NY pays ~1.3x avg, GA pays ~0.85x, etc.)
state_mult <- c(FL=0.87,NY=1.30,TX=0.94,CA=1.15,PA=1.05,OH=0.95,IL=0.98,
                GA=0.84,MN=1.10,AZ=0.92,MA=1.18,MI=0.93,NC=0.91,WA=1.08,
                CO=1.00,NJ=1.05,MD=1.03,VA=0.93,IN=0.90,OR=1.05)
mult <- ifelse(states %in% names(state_mult),
               state_mult[states],
               runif(length(states), 0.85, 1.15))

# Add provider-level random variation (±15%)
prov_var <- rnorm(length(npis), 1.0, 0.08)

# Calculate realistic values
adj_rate <- rates * as.numeric(mult) * prov_var
n_claims <- rpois(N_ROWS, lambda = ifelse(
  codes %in% c("T1019","T2025","T1020"), 500,  # HCBS = high volume
  ifelse(codes %in% c("99213","99214"), 200,    # E&M = moderate
         ifelse(codes %in% c("27447","59400","59510"), 5, # Surgery = low
                50))                              # Default
))
n_claims <- pmax(n_claims, 12)  # Minimum 12 (suppression threshold)
n_bene <- pmax(round(n_claims * runif(N_ROWS, 0.3, 0.9)), 11)

spending <- data.table(
  BILLING_PROVIDER_NPI_NUM = npis,
  SERVICING_PROVIDER_NPI_NUM = npis,  # Same for most rows; some differ
  HCPCS_CODE = codes,
  CLAIM_FROM_MONTH = months,
  TOTAL_UNIQUE_BENEFICIARIES = as.integer(n_bene),
  TOTAL_CLAIMS = as.integer(n_claims),
  TOTAL_PAID = round(adj_rate * n_claims, 2)
)

# Make ~10% have different billing vs servicing NPI
diff_idx <- sample(nrow(spending), round(nrow(spending) * 0.1))
spending[diff_idx, SERVICING_PROVIDER_NPI_NUM := gen_npi(length(diff_idx))]

# Remove exact duplicates (same NPI × code × month)
spending <- unique(spending, by = c("BILLING_PROVIDER_NPI_NUM", "HCPCS_CODE", "CLAIM_FROM_MONTH"))

cat("  Generated", nrow(spending), "spending records\n")
cat("  Total paid: $", format(sum(spending$TOTAL_PAID), big.mark = ",", digits = 10), "\n")
cat("  Total claims:", format(sum(spending$TOTAL_CLAIMS), big.mark = ","), "\n")
cat("  Avg $/claim: $", round(sum(spending$TOTAL_PAID) / sum(spending$TOTAL_CLAIMS), 2), "\n")
cat("  Unique NPIs:", uniqueN(spending$BILLING_PROVIDER_NPI_NUM), "\n")
cat("  Unique codes:", uniqueN(spending$HCPCS_CODE), "\n")
cat("  Month range:", min(spending$CLAIM_FROM_MONTH), "to", max(spending$CLAIM_FROM_MONTH), "\n")

# ── Step 3: Generate HCPCS Lookup Table ────────────────────────────────────
cat("\nStep 3: Generating HCPCS lookup...\n")

hcpcs_lookup <- CODES[, .(
  HCPCS_CODE = code,
  HCPCS_DESCRIPTION = desc,
  CATEGORY = cat
)]

cat("  HCPCS codes:", nrow(hcpcs_lookup), "\n")

# ── Step 4: Print Schema Summary ───────────────────────────────────────────
cat("\n═══════════════════════════════════════════════\n")
cat("  ACTUAL T-MSIS SCHEMA (opendata.hhs.gov)\n")
cat("═══════════════════════════════════════════════\n\n")

cat("spending table (227M rows in real data, ", nrow(spending), " in sample):\n")
cat("  ┌─────────────────────────────────┬──────────┬────────────────────────┐\n")
cat("  │ Column                          │ Type     │ Example                │\n")
cat("  ├─────────────────────────────────┼──────────┼────────────────────────┤\n")
cat("  │ BILLING_PROVIDER_NPI_NUM        │ string   │", spending$BILLING_PROVIDER_NPI_NUM[1], "      │\n")
cat("  │ SERVICING_PROVIDER_NPI_NUM      │ string   │", spending$SERVICING_PROVIDER_NPI_NUM[1], "      │\n")
cat("  │ HCPCS_CODE                      │ string   │", sprintf("%-14s", spending$HCPCS_CODE[1]), "        │\n")
cat("  │ CLAIM_FROM_MONTH                │ string   │", sprintf("%-14s", spending$CLAIM_FROM_MONTH[1]), "        │\n")
cat("  │ TOTAL_UNIQUE_BENEFICIARIES      │ int64    │", sprintf("%-14d", spending$TOTAL_UNIQUE_BENEFICIARIES[1]), "        │\n")
cat("  │ TOTAL_CLAIMS                    │ int64    │", sprintf("%-14d", spending$TOTAL_CLAIMS[1]), "        │\n")
cat("  │ TOTAL_PAID                      │ float64  │", sprintf("%-14.2f", spending$TOTAL_PAID[1]), "        │\n")
cat("  └─────────────────────────────────┴──────────┴────────────────────────┘\n\n")

cat("  ⚠ NO STATE COLUMN in spending table!\n")
cat("    State must be joined from provider tables via NPI.\n")
cat("    Options: (a) NPPES download, (b) billing_providers table,\n")
cat("    (c) servicing_providers table from the same HHS dataset.\n\n")

cat("billing_providers table (618K rows in real data, ", N_PROVIDERS, " in sample):\n")
cat("  NPI, PROVIDER_NAME, STATE, ENTITY_TYPE, TAXONOMY, ...\n\n")

cat("hcpcs_codes table (7.55K rows in real data, ", nrow(hcpcs_lookup), " in sample):\n")
cat("  HCPCS_CODE, HCPCS_DESCRIPTION, CATEGORY\n\n")

# ── Step 5: Write Files ───────────────────────────────────────────────────
cat("Step 5: Writing files...\n")

fwrite(spending, file.path(OUTPUT_DIR, "sample_spending.csv"))
cat("  → sample_spending.csv (", nrow(spending), " rows)\n")

fwrite(providers, file.path(OUTPUT_DIR, "sample_providers.csv"))
cat("  → sample_providers.csv (", N_PROVIDERS, " providers)\n")

fwrite(hcpcs_lookup, file.path(OUTPUT_DIR, "sample_hcpcs.csv"))
cat("  → sample_hcpcs.csv (", nrow(hcpcs_lookup), " codes)\n")

# ── Step 6: Quick Stats ───────────────────────────────────────────────────
cat("\n═══════════════════════════════════════════════\n")
cat("  Sample Data Summary\n")
cat("═══════════════════════════════════════════════\n\n")

# Top codes by total spend
cat("Top 10 HCPCS by Total Paid:\n")
top_codes <- spending[, .(
  total_paid = sum(TOTAL_PAID),
  total_claims = sum(TOTAL_CLAIMS),
  avg_per_claim = sum(TOTAL_PAID) / sum(TOTAL_CLAIMS),
  n_providers = uniqueN(BILLING_PROVIDER_NPI_NUM)
), by = HCPCS_CODE][order(-total_paid)][1:10]

for (i in 1:nrow(top_codes)) {
  cat(sprintf("  %5s  $%12s  %8s claims  $%8.2f/claim  %3d providers\n",
              top_codes$HCPCS_CODE[i],
              format(round(top_codes$total_paid[i]), big.mark = ","),
              format(top_codes$total_claims[i], big.mark = ","),
              top_codes$avg_per_claim[i],
              top_codes$n_providers[i]))
}

# Top states by total spend (joined)
cat("\nTop 10 States by Total Paid:\n")
spend_w_state <- merge(
  spending,
  providers[, .(NPI, STATE)],
  by.x = "BILLING_PROVIDER_NPI_NUM",
  by.y = "NPI",
  all.x = TRUE
)
top_states <- spend_w_state[!is.na(STATE), .(
  total_paid = sum(TOTAL_PAID),
  total_claims = sum(TOTAL_CLAIMS),
  n_providers = uniqueN(BILLING_PROVIDER_NPI_NUM)
), by = STATE][order(-total_paid)][1:10]

for (i in 1:nrow(top_states)) {
  cat(sprintf("  %2s  $%12s  %8s claims  %3d providers\n",
              top_states$STATE[i],
              format(round(top_states$total_paid[i]), big.mark = ","),
              format(top_states$total_claims[i], big.mark = ","),
              top_states$n_providers[i]))
}

cat("\n═══════════════════════════════════════════════\n")
cat("  Done! Test files ready.\n")
cat("  \n")
cat("  Next steps:\n")
cat("  1. Run tmsis_pipeline.R against sample_spending.csv\n")
cat("     Rscript tmsis_pipeline.R sample_spending.csv\n")
cat("  2. Check output JSON files in ../public/data/\n")
cat("  3. When ready, download real data from:\n")
cat("     https://opendata.hhs.gov/datasets/medicaid-provider-spending/\n")
cat("     Also grab billing_providers table for state mapping.\n")
cat("═══════════════════════════════════════════════\n")
