###############################################################################
# Aradune · T-MSIS Data Pipeline (DuckDB)
#
# Processes the full 227M-row T-MSIS provider spending dataset into
# dashboard-ready JSON — without loading it all into RAM.
#
# DuckDB streams from disk. 16GB RAM is plenty.
# Processes ALL HCPCS codes. Adds sub-state geography via NPPES.
#
# Input:  T-MSIS spending CSV/parquet from opendata.hhs.gov
#         + billing_providers file for state mapping
#         + (optional) NPPES file for ZIP/county granularity
#         + (optional) HCPCS reference CSV for code descriptions
#
# Output: JSON files in ../public/data/ for the React dashboard
#         states.json, hcpcs.json, trends.json, regions.json, meta.json
#
# Requirements: R 4.x, duckdb, jsonlite
#   Optional: readxl (for Excel RVU files)
# Install: Rscript -e 'install.packages(c("duckdb","jsonlite","readxl","data.table"))'
#
# Usage:
#   Rscript tmsis_pipeline_duckdb.R spending.csv
#   Rscript tmsis_pipeline_duckdb.R spending.csv nppes.csv
#   Rscript tmsis_pipeline_duckdb.R spending.csv providers.csv nppes.csv
#
# The pipeline auto-detects whether a file is NPPES or providers by filename.
# Files in the same directory are also auto-detected.
###############################################################################

library(duckdb)
library(jsonlite)

cat("\n═══════════════════════════════════════════════════\n")
cat("  Aradune · T-MSIS Pipeline (DuckDB)\n")
cat("═══════════════════════════════════════════════════\n\n")

# ── Configuration ──────────────────────────────────────────────────────────
args <- commandArgs(trailingOnly = TRUE)
SPENDING_FILE <- if (length(args) >= 1) args[1] else "sample_spending.csv"
PROVIDER_FILE <- NULL
NPPES_FILE    <- NULL

# Smart argument handling: detect NPPES vs provider files by filename
if (length(args) >= 2) {
  for (a in args[-1]) {
    if (grepl("(nppes|npidata|npi_file|NPI_Files)", a, ignore.case = TRUE)) {
      NPPES_FILE <- a
    } else if (is.null(PROVIDER_FILE)) {
      PROVIDER_FILE <- a
    }
  }
}

OUTPUT_DIR    <- "../public/data"
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ── Load HCPCS reference ──────────────────────────────────────────────────
# Priority order (later sources override earlier):
#   1. Built-in hcpcs_reference.R (~200 common codes)
#   2. CMS HCPCS Level II fixed-width text file (A-V codes, ~6,000 codes)
#   3. CMS PFS RVU file / Addendum B (CPT codes, ~10,000 codes)
#   4. Any user-provided CSV with code + description columns

HCPCS_REF <- NULL
input_dir <- dirname(SPENDING_FILE)
if (input_dir == ".") input_dir <- getwd()

merge_ref <- function(new_ref, existing) {
  # New codes take priority over existing
  if (is.null(existing)) return(new_ref)
  rbind(new_ref, existing[!existing$code %in% new_ref$code, ])
}

# ── Source 1: Built-in R reference ──
# Find script directory robustly
script_dir <- tryCatch({
  args_all <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args_all, value = TRUE)
  if (length(file_arg) > 0) dirname(sub("^--file=", "", file_arg[1]))
  else getwd()
}, error = function(e) getwd())

ref_path <- file.path(script_dir, "hcpcs_reference.R")
if (!file.exists(ref_path)) ref_path <- "hcpcs_reference.R"
if (!file.exists(ref_path)) ref_path <- file.path(input_dir, "hcpcs_reference.R")

if (file.exists(ref_path)) {
  source(ref_path, local = TRUE)
  if (exists("hcpcs_reference")) {
    HCPCS_REF <- hcpcs_reference
    cat("  [ref] Built-in reference:", nrow(HCPCS_REF), "codes\n")
  }
}

# ── Source 2: CMS HCPCS Level II fixed-width file ──
# Format: 5-char code, skip 6, 80-char long desc, 28-char short desc
# Download from: https://www.hhs.gov/guidance/document/hcpcs-quarterly-update
# The ZIP contains a .txt file like "HCPC2026_JAN_ANWEB.txt"
cms_fwf_candidates <- list.files(input_dir, pattern = "(?i)(hcpc|anweb).*\\.txt$",
                                  full.names = TRUE)
# Also check for the unzipped ZIP contents
cms_zip_candidates <- list.files(input_dir,
  pattern = "(?i)(alpha-numeric|hcpcs.*file).*\\.zip$", full.names = TRUE)

for (zf in cms_zip_candidates) {
  # Unzip to temp, find the .txt file inside
  tmp <- tempdir()
  unzip(zf, exdir = tmp)
  txt_inside <- list.files(tmp, pattern = "(?i)(hcpc|anweb).*\\.txt$",
                           full.names = TRUE, recursive = TRUE)
  cms_fwf_candidates <- c(cms_fwf_candidates, txt_inside)
}

for (f in cms_fwf_candidates) {
  tryCatch({
    # Read fixed-width: code(5), seq(5), rectype(1), longdesc(80), shortdesc(28)
    raw <- readLines(f, warn = FALSE)
    # Filter to record type 3 (procedure codes) — char at position 11
    # But some files don't have record types; try both approaches
    parsed <- data.frame(
      code = trimws(substr(raw, 1, 5)),
      long_desc = trimws(substr(raw, 12, 91)),
      short_desc = trimws(substr(raw, 92, 119)),
      stringsAsFactors = FALSE
    )
    # Keep only rows that look like HCPCS codes (letter + 4 digits)
    parsed <- parsed[grepl("^[A-Z][0-9]{4}$", parsed$code), ]
    # Use long description, fall back to short if empty
    parsed$desc <- ifelse(nchar(parsed$long_desc) > 0,
                          parsed$long_desc, parsed$short_desc)
    parsed <- parsed[nchar(parsed$desc) > 0, c("code", "desc")]
    
    if (nrow(parsed) > 100) {
      HCPCS_REF <- merge_ref(parsed, HCPCS_REF)
      cat("  [ref] CMS HCPCS Level II:", nrow(parsed), "codes from",
          basename(f), "\n")
      break
    }
  }, error = function(e) {
    # Silently skip files that don't parse
  })
}

# ── Source 3: CMS PFS RVU file (Addendum B) ──
# This is usually an Excel file with columns like HCPCS, DESCRIPTION, WORK RVU...
# Download from CMS PFS final rule data files:
#   https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files
# Also handles CSV exports of the same data.
rvu_candidates <- list.files(input_dir, pattern = "(?i)(rvu|addendum.*b|pprrvu|RVU).*\\.(xlsx|xls|csv)$",
                              full.names = TRUE)
for (f in rvu_candidates) {
  tryCatch({
    is_excel <- grepl("\\.(xlsx|xls)$", f, ignore.case = TRUE)
    
    if (is_excel) {
      # Try readxl if available, otherwise openxlsx
      if (requireNamespace("readxl", quietly = TRUE)) {
        rvu_raw <- readxl::read_excel(f, col_types = "text")
      } else if (requireNamespace("openxlsx", quietly = TRUE)) {
        rvu_raw <- openxlsx::read.xlsx(f)
      } else {
        cat("  [ref] Found RVU Excel file but no Excel reader installed.\n")
        cat("        Install with: install.packages('readxl')\n")
        cat("        Or save as CSV and re-run.\n")
        next
      }
    } else {
      rvu_raw <- read.csv(f, stringsAsFactors = FALSE, check.names = FALSE)
    }
    
    # Detect HCPCS and description columns
    col_names_lower <- tolower(names(rvu_raw))
    hcpcs_col <- which(col_names_lower %in% c("hcpcs","hcpcs code","cpt/hcpcs",
                       "cpt","code","procedure code"))[1]
    desc_col <- which(col_names_lower %in% c("description","short description",
                      "descriptor","mod description","long description"))[1]
    
    if (!is.na(hcpcs_col) && !is.na(desc_col)) {
      rvu_ref <- data.frame(
        code = trimws(as.character(rvu_raw[[hcpcs_col]])),
        desc = trimws(as.character(rvu_raw[[desc_col]])),
        stringsAsFactors = FALSE
      )
      # Keep valid codes only
      rvu_ref <- rvu_ref[grepl("^[0-9]{5}$|^[A-Z][0-9]{4}$", rvu_ref$code) &
                          nchar(rvu_ref$desc) > 0, ]
      rvu_ref <- rvu_ref[!duplicated(rvu_ref$code), ]
      
      if (nrow(rvu_ref) > 100) {
        HCPCS_REF <- merge_ref(rvu_ref, HCPCS_REF)
        cat("  [ref] CMS RVU/Addendum B:", nrow(rvu_ref), "codes from",
            basename(f), "\n")
        break
      }
    }
  }, error = function(e) {
    # Skip files that don't parse
  })
}

# ── Source 4: Any user-provided CSV ──
csv_ref_candidates <- c(
  file.path(input_dir, "hcpcs_codes.csv"),
  file.path(input_dir, "hcpcs_reference.csv"),
  file.path(input_dir, "cpt_hcpcs.csv"),
  file.path(input_dir, "code_descriptions.csv")
)
for (f in csv_ref_candidates) {
  if (file.exists(f)) {
    tryCatch({
      ext <- read.csv(f, stringsAsFactors = FALSE)
      code_col <- names(ext)[grep("code|hcpcs|cpt", names(ext), ignore.case = TRUE)[1]]
      desc_col <- names(ext)[grep("desc|name|label|short", names(ext), ignore.case = TRUE)[1]]
      if (!is.na(code_col) && !is.na(desc_col)) {
        ext_ref <- data.frame(code = trimws(ext[[code_col]]),
                              desc = trimws(ext[[desc_col]]),
                              stringsAsFactors = FALSE)
        ext_ref <- ext_ref[nchar(ext_ref$desc) > 0, ]
        HCPCS_REF <- merge_ref(ext_ref, HCPCS_REF)
        cat("  [ref] User CSV:", nrow(ext_ref), "codes from", basename(f), "\n")
      }
    }, error = function(e) {})
    break
  }
}

total_ref <- if (!is.null(HCPCS_REF)) nrow(HCPCS_REF) else 0
cat("  Total reference codes:", total_ref, "\n")

get_desc <- function(code) {
  if (!is.null(HCPCS_REF)) {
    m <- HCPCS_REF$desc[HCPCS_REF$code == code]
    if (length(m) > 0 && !is.na(m[1])) return(m[1])
  }
  return(NA_character_)
}

# ── State FMAP (FY2025) ──────────────────────────────────────────────────
FMAP <- data.frame(
  state = c("AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID",
            "IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO",
            "MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA",
            "RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"),
  fmap = c(72.58,50,70.84,74.67,50,50,50,57.06,70,58.59,67.03,51.86,71.01,
           50.88,66.93,61.30,57.73,72.19,73.08,63.11,50,50,65.60,50,77.76,
           64.04,65.52,55.94,64.03,50,50,73.04,50,66.68,50,62.36,67.82,60.14,
           52.31,52.86,71.43,59.12,65.06,61.80,68.33,56.18,50,50,74.99,59.36,50),
  stringsAsFactors = FALSE
)

# ── HCPCS category mapping ──────────────────────────────────────────────
hcpcs_category <- function(code) {
  prefix <- substr(code, 1, 1)
  num5 <- grepl("^[0-9]{5}$", code)
  if (num5) {
    c <- as.integer(code)
    if (c >= 99201 & c <= 99499) return("E&M")
    if (c >= 90832 & c <= 90853) return("Behavioral")
    if (c >= 96130 & c <= 96171) return("Behavioral")
    if (c >= 97151 & c <= 97158) return("Behavioral")
    if (c >= 59000 & c <= 59899) return("Maternity")
    if (c >= 70010 & c <= 79999) return("Imaging")
    if (c >= 10004 & c <= 69990) return("Surgery")
    if (c >= 80047 & c <= 89398) return("Lab/Path")
    if (c >= 90281 & c <= 90399) return("Immunization")
    if (c >= 90460 & c <= 90474) return("Immunization")
    if (c >= 90785 & c <= 90899) return("Behavioral")
    if (c >= 91010 & c <= 91299) return("Diagnostic")
    if (c >= 92002 & c <= 92499) return("Vision")
    if (c >= 92502 & c <= 92700) return("Audiology")
    if (c >= 96360 & c <= 96549) return("Infusion")
    if (c >= 97010 & c <= 97799) return("Rehab/Therapy")
    if (c >= 99500 & c <= 99607) return("Home Services")
    return("Procedure")
  }
  if (prefix == "D") return("Dental")
  if (prefix == "J") return("Drugs")
  if (prefix == "T") return("HCBS/Waiver")
  if (prefix == "S") return("HCBS/Waiver")
  if (prefix == "H") return("Behavioral")
  if (prefix == "G") return("Temporary/CMS")
  if (prefix == "A") return("DME/Supply")
  if (prefix == "E") return("DME/Supply")
  if (prefix == "L") return("Orthotics")
  if (prefix == "V") return("Vision")
  if (prefix == "Q") return("Temporary/CMS")
  if (prefix == "K") return("DME/Supply")
  if (prefix == "C") return("Outpatient APC")
  if (prefix == "R") return("Diagnostic")
  return("Other")
}

# ── ZIP3 Region Names ────────────────────────────────────────────────────
# Major ZIP3 areas for readable region labels
# This covers the largest metro areas; unlisted ZIP3s get "ZIP3 XXX"
ZIP3_NAMES <- c(
  "100"="New York City","101"="New York City","104"="New York City",
  "112"="Brooklyn","113"="Queens/Flushing","110"="Queens",
  "111"="Long Island City","114"="Jamaica","115"="Bronx",
  "116"="Far Rockaway","117"="Hicksville","118"="Hauppauge",
  "900"="Los Angeles","901"="Los Angeles","902"="Inglewood",
  "903"="Inglewood","906"="Whittier","907"="Long Beach",
  "908"="Long Beach","910"="Pasadena","913"="Van Nuys",
  "914"="Van Nuys","917"="Industry","918"="Alhambra",
  "606"="Chicago","607"="Chicago","608"="Chicago South",
  "600"="North Suburban IL","601"="Carol Stream",
  "770"="Houston","771"="Houston","772"="Houston",
  "773"="Conroe","774"="Wharton",
  "191"="Philadelphia","190"="Philadelphia","192"="Southeastern PA",
  "441"="Cleveland","442"="Akron","443"="Akron",
  "432"="Columbus","430"="Columbus","431"="Columbus",
  "452"="Cincinnati","450"="Cincinnati","451"="Cincinnati",
  "303"="Atlanta","300"="Atlanta","301"="Atlanta",
  "302"="Atlanta","304"="Swainsboro","305"="Gainesville",
  "331"="Miami","330"="Miami","332"="Fort Lauderdale",
  "333"="Fort Lauderdale","334"="West Palm Beach",
  "320"="Jacksonville","321"="Daytona","322"="Gainesville",
  "323"="Tallahassee","324"="Panama City","325"="Pensacola",
  "326"="Gainesville","327"="Orlando","328"="Orlando","329"="Melbourne",
  "335"="Tampa","336"="Tampa","337"="St Petersburg",
  "338"="Lakeland","339"="Fort Myers","340"="Fort Myers",
  "341"="Sarasota","342"="Manasota","343"="Panama City","344"="Ocala",
  "346"="Tampa","347"="Orlando","349"="Fort Pierce",
  "551"="Minneapolis","553"="Minneapolis","554"="Minneapolis",
  "550"="St Paul","555"="St Paul","560"="Mankato",
  "480"="Detroit","481"="Detroit","482"="Detroit",
  "483"="Royal Oak","484"="Flint","485"="Flint",
  "750"="Dallas","751"="Dallas","752"="Fort Worth",
  "753"="Fort Worth","760"="Fort Worth",
  "782"="San Antonio","781"="San Antonio",
  "787"="Austin","786"="Austin",
  "733"="Brownsville","734"="Corpus Christi",
  "799"="El Paso","798"="El Paso",
  "941"="San Francisco","940"="San Francisco",
  "943"="Palo Alto","945"="Oakland","946"="Oakland",
  "950"="San Jose","951"="San Bernardino","952"="Stockton",
  "920"="San Diego","921"="San Diego",
  "956"="Sacramento","957"="Sacramento","958"="Sacramento",
  "080"="Newark","070"="Newark","071"="Newark",
  "076"="Hackensack","074"="Paterson",
  "200"="Washington DC","201"="Dulles","206"="Washington DC",
  "207"="Southern MD","208"="Suburban MD","209"="Silver Spring",
  "210"="Baltimore","211"="Baltimore","212"="Baltimore",
  "220"="Northern VA","221"="Northern VA","222"="Arlington",
  "223"="Alexandria","230"="Richmond","231"="Richmond",
  "462"="Indianapolis","460"="Indianapolis","461"="Indianapolis",
  "981"="Seattle","980"="Seattle","982"="Everett",
  "983"="Tacoma","984"="Tacoma","986"="Portland",
  "972"="Portland","970"="Portland","973"="Salem",
  "802"="Denver","800"="Denver","801"="Denver","803"="Boulder",
  "852"="Phoenix","850"="Phoenix","851"="Phoenix",
  "853"="Phoenix","857"="Tucson","856"="Tucson"
)

get_zip3_name <- function(z3, state) {
  nm <- ZIP3_NAMES[z3]
  if (!is.na(nm)) return(nm)
  return(paste0("ZIP3 ", z3))
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: LOCATE FILES
# ═══════════════════════════════════════════════════════════════════════════
cat("Step 1: Locating files...\n")

if (!file.exists(SPENDING_FILE)) {
  cat("\n  ERROR: Spending file not found:", SPENDING_FILE, "\n")
  cat("  Download from: https://opendata.hhs.gov/datasets/medicaid-provider-spending/\n\n")
  quit(status = 1)
}
cat("  Spending:", SPENDING_FILE,
    "(", round(file.size(SPENDING_FILE)/1e9, 2), "GB)\n")

is_parquet <- grepl("\\.(parquet|pq)$", SPENDING_FILE, ignore.case = TRUE)
input_dir <- dirname(SPENDING_FILE)
if (input_dir == ".") input_dir <- getwd()

# Auto-find provider file
if (is.null(PROVIDER_FILE)) {
  for (f in c("sample_providers.csv","billing_providers.csv",
              "servicing_providers.csv","providers.csv",
              "billing_providers.parquet")) {
    fp <- file.path(input_dir, f)
    if (file.exists(fp)) { PROVIDER_FILE <- fp; break }
  }
}

has_providers <- !is.null(PROVIDER_FILE) && file.exists(PROVIDER_FILE)
if (has_providers) cat("  Providers:", PROVIDER_FILE, "\n")

# Auto-find NPPES file
if (is.null(NPPES_FILE)) {
  # Check for exact names first, then glob for npidata_pfile_*.csv
  for (f in c("nppes.csv","npidata.csv","npidata_pfile.csv",
              "NPPES_Data_Dissemination.csv")) {
    fp <- file.path(input_dir, f)
    if (file.exists(fp)) { NPPES_FILE <- fp; break }
    fp2 <- file.path(dirname(input_dir), f)
    if (file.exists(fp2)) { NPPES_FILE <- fp2; break }
  }
  # Glob for npidata_pfile_*.csv (e.g. npidata_pfile_20050523-20260208.csv)
  if (is.null(NPPES_FILE)) {
    npi_glob <- Sys.glob(file.path(input_dir, "npidata_pfile_*.csv"))
    if (length(npi_glob) > 0) NPPES_FILE <- npi_glob[1]
  }
}

has_nppes <- !is.null(NPPES_FILE) && file.exists(NPPES_FILE)
if (has_nppes) {
  cat("  NPPES:", NPPES_FILE,
      "(", round(file.size(NPPES_FILE)/1e9, 2), "GB)\n")
} else {
  cat("  NPPES: not found (sub-state regions will be skipped)\n")
  cat("         Download from: https://download.cms.gov/nppes/NPI_Files.html\n")
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: CONNECT DUCKDB
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 2: Connecting DuckDB...\n")

con <- dbConnect(duckdb())

# Memory limit — leave headroom for R + OS on 16GB machines
dbExecute(con, "SET memory_limit='6GB'")
dbExecute(con, "SET threads TO 4")
cat("  DuckDB: 6GB memory limit, 4 threads\n")

# Register spending
if (is_parquet) {
  dbExecute(con, sprintf("CREATE VIEW raw_spending AS SELECT * FROM read_parquet('%s')", SPENDING_FILE))
} else {
  dbExecute(con, sprintf("CREATE VIEW raw_spending AS SELECT * FROM read_csv_auto('%s', sample_size=100000)", SPENDING_FILE))
}

cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'raw_spending'")
cat("  Columns:", paste(cols$column_name, collapse = ", "), "\n")

# Detect columns
detect_col <- function(candidates, available) {
  for (c in candidates) {
    match <- available[tolower(available) == tolower(c)]
    if (length(match) > 0) return(match[1])
  }
  return(NULL)
}

col_npi    <- detect_col(c("BILLING_PROVIDER_NPI_NUM","billing_npi","npi","NPI"), cols$column_name)
col_hcpcs  <- detect_col(c("HCPCS_CODE","hcpcs_code","hcpcs","HCPCS"), cols$column_name)
col_pay    <- detect_col(c("TOTAL_PAID","total_paid","payment","paid_amount"), cols$column_name)
col_claims <- detect_col(c("TOTAL_CLAIMS","total_claims","claims","claim_count"), cols$column_name)
col_bene   <- detect_col(c("TOTAL_UNIQUE_BENEFICIARIES","total_unique_beneficiaries","beneficiaries"), cols$column_name)
col_month  <- detect_col(c("CLAIM_FROM_MONTH","claim_from_month","month","Year_Month"), cols$column_name)
col_state  <- detect_col(c("STATE","state","state_code","submitting_state"), cols$column_name)

cat("  Mapped → npi:", col_npi, " hcpcs:", col_hcpcs, " pay:", col_pay,
    " claims:", col_claims, " month:", col_month, " state:", col_state, "\n")

if (is.null(col_hcpcs) || is.null(col_pay)) {
  cat("\n  ERROR: Could not find HCPCS or payment columns.\n")
  dbDisconnect(con, shutdown = TRUE); quit(status = 1)
}

row_count <- dbGetQuery(con, "SELECT COUNT(*) as n FROM raw_spending")$n
cat("  Rows:", format(row_count, big.mark = ","), "\n")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: STATE + GEOGRAPHY JOINS
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 3: State + geography mapping...\n")

# Start building the main view
has_zip <- FALSE

  if (!is.null(col_state)) {
  # State already in spending data
  cat("  State column found directly in spending data.\n")
  
  # Build state alias — avoid duplicate if column is already named "state"
  state_needs_alias <- tolower(col_state) != "state"
  state_select <- if (state_needs_alias) {
    sprintf(", \"%s\" AS state", col_state)
  } else ""
  state_exclude <- if (state_needs_alias) {
    sprintf(" EXCLUDE (\"%s\")", col_state)
  } else ""
  
  # If NPPES is available, join it for ZIP3 geography even though we have state
  if (has_nppes) {
    cat("  NPPES available — joining for sub-state ZIP3 regions...\n")
    is_nppes_parquet <- grepl("\\.(parquet|pq)$", NPPES_FILE, ignore.case = TRUE)
    if (is_nppes_parquet) {
      dbExecute(con, sprintf("CREATE VIEW nppes_raw AS SELECT * FROM read_parquet('%s')", NPPES_FILE))
    } else {
      dbExecute(con, sprintf("CREATE VIEW nppes_raw AS SELECT * FROM read_csv_auto('%s', sample_size=50000)", NPPES_FILE))
    }
    
    nppes_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'nppes_raw'")
    
    np_npi   <- detect_col(c("NPI","npi"), nppes_cols$column_name)
    np_zip   <- detect_col(c("Provider Business Practice Location Address Postal Code",
                              "provider_business_practice_location_address_postal_code",
                              "ZIP","zip","practice_zip"), nppes_cols$column_name)
    np_city  <- detect_col(c("Provider Business Practice Location Address City Name",
                              "provider_business_practice_location_address_city_name",
                              "CITY","city","practice_city"), nppes_cols$column_name)
    
    if (!is.null(np_npi) && !is.null(np_zip) && !is.null(col_npi)) {
      city_select_npi <- if (!is.null(np_city)) sprintf(", \"%s\" AS city", np_city) else ""
      dbExecute(con, sprintf("
        CREATE VIEW npi_geo AS
        SELECT DISTINCT
          CAST(\"%s\" AS VARCHAR) AS npi,
          LEFT(CAST(\"%s\" AS VARCHAR), 3) AS zip3
          %s
        FROM nppes_raw
        WHERE \"%s\" IS NOT NULL
      ", np_npi, np_zip, city_select_npi, np_zip))
      
      npi_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM npi_geo")$n
      cat("  NPPES loaded:", format(npi_count, big.mark = ","), "NPIs with ZIP3\n")
      
      city_join <- if (!is.null(np_city)) ", g.city" else ""
      dbExecute(con, sprintf("
        CREATE VIEW spending AS
        SELECT s.*%s %s, g.zip3 %s
        FROM raw_spending s
        LEFT JOIN npi_geo g ON CAST(s.\"%s\" AS VARCHAR) = g.npi
        WHERE CAST(s.\"%s\" AS DOUBLE) > 0
      ", state_exclude, state_select, city_join, col_npi, col_pay))
      
      has_zip <- TRUE
      cat("  Sub-state geography: ZIP3 regions enabled\n")
    } else {
      cat("  NPPES columns not detected for ZIP — using state only.\n")
      dbExecute(con, sprintf("CREATE VIEW spending AS SELECT *%s %s FROM raw_spending WHERE CAST(\"%s\" AS DOUBLE) > 0",
                             state_exclude, state_select, col_pay))
    }
  } else {
    dbExecute(con, sprintf("CREATE VIEW spending AS SELECT *%s %s FROM raw_spending WHERE CAST(\"%s\" AS DOUBLE) > 0",
                           state_exclude, state_select, col_pay))
  }

} else if (has_providers || has_nppes) {
  # Need to join for state
  
  if (has_nppes) {
    cat("  Loading NPPES for state + ZIP...\n")
    is_nppes_parquet <- grepl("\\.(parquet|pq)$", NPPES_FILE, ignore.case = TRUE)
    if (is_nppes_parquet) {
      dbExecute(con, sprintf("CREATE VIEW nppes_raw AS SELECT * FROM read_parquet('%s')", NPPES_FILE))
    } else {
      dbExecute(con, sprintf("CREATE VIEW nppes_raw AS SELECT * FROM read_csv_auto('%s', sample_size=50000)", NPPES_FILE))
    }
    
    nppes_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'nppes_raw'")
    
    np_npi   <- detect_col(c("NPI","npi"), nppes_cols$column_name)
    np_state <- detect_col(c("Provider Business Practice Location Address State Name",
                              "provider_business_practice_location_address_state_name",
                              "STATE","state","practice_state"), nppes_cols$column_name)
    np_zip   <- detect_col(c("Provider Business Practice Location Address Postal Code",
                              "provider_business_practice_location_address_postal_code",
                              "ZIP","zip","practice_zip"), nppes_cols$column_name)
    np_city  <- detect_col(c("Provider Business Practice Location Address City Name",
                              "provider_business_practice_location_address_city_name",
                              "CITY","city","practice_city"), nppes_cols$column_name)
    
    if (!is.null(np_npi) && !is.null(np_state)) {
      zip_select <- ""
      city_select <- ""
      if (!is.null(np_zip)) {
        zip_select <- sprintf(", LEFT(CAST(\"%s\" AS VARCHAR), 3) AS zip3", np_zip)
        has_zip <- TRUE
      }
      if (!is.null(np_city)) {
        city_select <- sprintf(", \"%s\" AS city", np_city)
      }
      
      # ── State normalization lookup ──
      # NPPES has free-text state fields: "Florida", "FLORIDA", "FL",
      # "CA - CALIFORNIA", "S. Carolina", plus foreign entries (BAYERN, ONTARIO).
      # Normalize everything to standard 2-letter codes, drop non-US.
      cat("  Building state normalization lookup...\n")
      dbExecute(con, "
        CREATE TABLE state_lookup AS
        SELECT * FROM (VALUES
          ('AL','AL'),('Alabama','AL'),('ALABAMA','AL'),
          ('AK','AK'),('Alaska','AK'),('ALASKA','AK'),
          ('AZ','AZ'),('Arizona','AZ'),('ARIZONA','AZ'),
          ('AR','AR'),('Arkansas','AR'),('ARKANSAS','AR'),
          ('CA','CA'),('California','CA'),('CALIFORNIA','CA'),('CA - CALIFORNIA','CA'),
          ('CO','CO'),('Colorado','CO'),('COLORADO','CO'),('CO- COLORADO','CO'),('C0','CO'),
          ('CT','CT'),('Connecticut','CT'),('CONNECTICUT','CT'),
          ('DE','DE'),('Delaware','DE'),('DELAWARE','DE'),
          ('DC','DC'),('D.C.','DC'),('District of Columbia','DC'),('DISTRICT OF COLUMBIA','DC'),
          ('FL','FL'),('Florida','FL'),('FLORIDA','FL'),
          ('GA','GA'),('Georgia','GA'),('GEORGIA','GA'),
          ('HI','HI'),('Hawaii','HI'),('HAWAII','HI'),
          ('ID','ID'),('Idaho','ID'),('IDAHO','ID'),
          ('IL','IL'),('Illinois','IL'),('ILLINOIS','IL'),
          ('IN','IN'),('Indiana','IN'),('INDIANA','IN'),
          ('IA','IA'),('Iowa','IA'),('IOWA','IA'),
          ('KS','KS'),('Kansas','KS'),('KANSAS','KS'),
          ('KY','KY'),('Kentucky','KY'),('KENTUCKY','KY'),
          ('LA','LA'),('Louisiana','LA'),('LOUISIANA','LA'),
          ('ME','ME'),('Maine','ME'),('MAINE','ME'),
          ('MD','MD'),('Maryland','MD'),('MARYLAND','MD'),('MD-MARYLAND','MD'),
          ('MA','MA'),('Massachusetts','MA'),('MASSACHUSETTS','MA'),
          ('MI','MI'),('Michigan','MI'),('MICHIGAN','MI'),
          ('MN','MN'),('Minnesota','MN'),('MINNESOTA','MN'),
          ('MS','MS'),('Mississippi','MS'),('MISSISSIPPI','MS'),
          ('MO','MO'),('Missouri','MO'),('MISSOURI','MO'),
          ('MT','MT'),('Montana','MT'),('MONTANA','MT'),
          ('NE','NE'),('Nebraska','NE'),('NEBRASKA','NE'),
          ('NV','NV'),('Nevada','NV'),('NEVADA','NV'),
          ('NH','NH'),('New Hampshire','NH'),('NEW HAMPSHIRE','NH'),
          ('NJ','NJ'),('New Jersey','NJ'),('NEW JERSEY','NJ'),
          ('NM','NM'),('New Mexico','NM'),('NEW MEXICO','NM'),
          ('NY','NY'),('New York','NY'),('NEW YORK','NY'),
          ('NC','NC'),('North Carolina','NC'),('NORTH CAROLINA','NC'),('N. Carolina','NC'),('N Carolina','NC'),
          ('ND','ND'),('North Dakota','ND'),('NORTH DAKOTA','ND'),('N. Dakota','ND'),('N Dakota','ND'),
          ('OH','OH'),('Ohio','OH'),('OHIO','OH'),
          ('OK','OK'),('Oklahoma','OK'),('OKLAHOMA','OK'),
          ('OR','OR'),('Oregon','OR'),('OREGON','OR'),
          ('PA','PA'),('Pennsylvania','PA'),('PENNSYLVANIA','PA'),
          ('RI','RI'),('Rhode Island','RI'),('RHODE ISLAND','RI'),
          ('SC','SC'),('South Carolina','SC'),('SOUTH CAROLINA','SC'),('S. Carolina','SC'),('S Carolina','SC'),
          ('SD','SD'),('South Dakota','SD'),('SOUTH DAKOTA','SD'),('S. Dakota','SD'),('S Dakota','SD'),
          ('TN','TN'),('Tennessee','TN'),('TENNESSEE','TN'),
          ('TX','TX'),('Texas','TX'),('TEXAS','TX'),
          ('UT','UT'),('Utah','UT'),('UTAH','UT'),
          ('VT','VT'),('Vermont','VT'),('VERMONT','VT'),
          ('VA','VA'),('Virginia','VA'),('VIRGINIA','VA'),
          ('WA','WA'),('Washington','WA'),('WASHINGTON','WA'),
          ('WV','WV'),('West Virginia','WV'),('WEST VIRGINIA','WV'),('W. Virginia','WV'),('W Virginia','WV'),
          ('WI','WI'),('Wisconsin','WI'),('WISCONSIN','WI'),
          ('WY','WY'),('Wyoming','WY'),('WYOMING','WY'),
          ('PR','PR'),('Puerto Rico','PR'),('PUERTO RICO','PR'),('PUESRTO RICO','PR'),('P.R.','PR'),
          ('GU','GU'),('Guam','GU'),('GUAM','GU'),
          ('VI','VI'),('Virgin Islands','VI'),('VIRGIN ISLANDS','VI'),
          ('AS','AS'),('American Samoa','AS'),('AMERICAN SAMOA','AS'),
          ('MP','MP'),('Northern Mariana Islands','MP'),('NORTHERN MARIANA ISLANDS','MP')
        ) AS t(raw_state, std_state)
      ")
      n_lookup <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM state_lookup")$n
      cat("  State lookup:", n_lookup, "variant mappings\n")
      
      dbExecute(con, sprintf("
        CREATE VIEW npi_geo AS
        SELECT DISTINCT
          CAST(\"%s\" AS VARCHAR) AS npi,
          sl.std_state AS state
          %s %s
        FROM nppes_raw n
        INNER JOIN state_lookup sl ON TRIM(n.\"%s\") = sl.raw_state
        WHERE n.\"%s\" IS NOT NULL
      ", np_npi, zip_select, city_select, np_state, np_state))
      
      npi_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM npi_geo")$n
      dropped <- dbGetQuery(con, sprintf("
        SELECT COUNT(DISTINCT CAST(\"%s\" AS VARCHAR)) AS n FROM nppes_raw
        WHERE TRIM(\"%s\") NOT IN (SELECT raw_state FROM state_lookup)
          AND \"%s\" IS NOT NULL
      ", np_npi, np_state, np_state))$n
      cat("  NPPES loaded:", format(npi_count, big.mark = ","), "NPIs with valid US state\n")
      cat("  Dropped:", format(dropped, big.mark = ","), "NPIs with non-US/invalid state\n")
      
      dbExecute(con, sprintf("
        CREATE VIEW spending AS
        SELECT s.*, g.state %s %s
        FROM raw_spending s
        JOIN npi_geo g ON CAST(s.%s AS VARCHAR) = g.npi
        WHERE CAST(s.%s AS DOUBLE) > 0
      ", if (has_zip) ", g.zip3" else "",
         if (!is.null(np_city)) ", g.city" else "",
         col_npi, col_pay))
      
    } else {
      cat("  WARNING: NPPES columns not detected. Falling back to provider file.\n")
      has_nppes <- FALSE
    }
  }
  
  if (!has_nppes && has_providers) {
    cat("  Joining state from provider file...\n")
    is_prov_parquet <- grepl("\\.(parquet|pq)$", PROVIDER_FILE, ignore.case = TRUE)
    if (is_prov_parquet) {
      dbExecute(con, sprintf("CREATE VIEW providers AS SELECT * FROM read_parquet('%s')", PROVIDER_FILE))
    } else {
      dbExecute(con, sprintf("CREATE VIEW providers AS SELECT * FROM read_csv_auto('%s')", PROVIDER_FILE))
    }
    
    prov_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'providers'")
    pcol_npi   <- detect_col(c("NPI","npi","BILLING_PROVIDER_NPI_NUM"), prov_cols$column_name)
    pcol_state <- detect_col(c("STATE","state","State","PROVIDER_STATE"), prov_cols$column_name)
    
    if (is.null(pcol_npi) || is.null(pcol_state)) {
      cat("  ERROR: Could not find NPI/STATE in provider file.\n")
      dbDisconnect(con, shutdown = TRUE); quit(status = 1)
    }
    
    dbExecute(con, sprintf("
      CREATE VIEW spending AS
      SELECT s.*, p.%s AS state
      FROM raw_spending s
      JOIN (SELECT DISTINCT %s AS join_npi, %s FROM providers) p
      ON CAST(s.%s AS VARCHAR) = CAST(p.join_npi AS VARCHAR)
      WHERE CAST(s.%s AS DOUBLE) > 0 AND p.%s IS NOT NULL
    ", pcol_state, pcol_npi, pcol_state, col_npi, col_pay, pcol_state))
  }
  
} else {
  cat("  ERROR: No state column and no provider/NPPES file.\n")
  dbDisconnect(con, shutdown = TRUE); quit(status = 1)
}

matched <- dbGetQuery(con, "SELECT COUNT(*) as n FROM spending")$n
cat("  Spending rows with state:", format(matched, big.mark = ","),
    "(", round(matched/row_count*100, 1), "%)\n")
if (has_zip) cat("  Sub-state geography: ZIP3 regions available\n")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4: SCAN ALL HCPCS CODES
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 4: Scanning all HCPCS codes...\n")

claims_expr <- if (!is.null(col_claims)) sprintf("SUM(CAST(%s AS BIGINT))", col_claims) else "COUNT(*)"
bene_expr   <- if (!is.null(col_bene)) sprintf("SUM(CAST(%s AS BIGINT))", col_bene) else "NULL"
npi_expr    <- if (!is.null(col_npi)) sprintf("COUNT(DISTINCT %s)", col_npi) else "NULL"

all_codes <- dbGetQuery(con, sprintf("
  SELECT
    %s AS code,
    COUNT(*) AS n_rows,
    SUM(CAST(%s AS DOUBLE)) AS total_paid,
    %s AS total_claims
  FROM spending
  WHERE CAST(%s AS DOUBLE) > 0
  GROUP BY %s
  ORDER BY total_paid DESC
", col_hcpcs, col_pay, claims_expr, col_pay, col_hcpcs))

cat("  Unique HCPCS codes:", format(nrow(all_codes), big.mark = ","), "\n")
cat("  Total spending: $", format(sum(all_codes$total_paid) / 1e9, digits = 4), "B\n")
cat("  Top 5 by spend:\n")
for (i in 1:min(5, nrow(all_codes))) {
  desc_txt <- get_desc(all_codes$code[i])
  if (is.na(desc_txt)) desc_txt <- ""
  cat(sprintf("    %s %-35s $%sB  %s claims\n",
              all_codes$code[i], desc_txt,
              format(round(all_codes$total_paid[i] / 1e9, 2), nsmall = 2),
              format(all_codes$total_claims[i], big.mark = ",")))
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 5: STATE × HCPCS RATES
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 5: Computing state × HCPCS rates...\n")
t0 <- Sys.time()

rate_by_state <- dbGetQuery(con, sprintf("
  SELECT
    %s AS code,
    state,
    SUM(CAST(%s AS DOUBLE)) / NULLIF(%s, 0) AS avg_rate,
    SUM(CAST(%s AS DOUBLE)) AS total_paid,
    %s AS total_claims,
    %s AS total_bene,
    %s AS n_providers
  FROM spending
  GROUP BY %s, state
", col_hcpcs, col_pay, claims_expr, col_pay, claims_expr, bene_expr, npi_expr,
   col_hcpcs))

elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
cat(sprintf("  Done in %.1f seconds\n", elapsed))
cat("  State × code combos:", format(nrow(rate_by_state), big.mark = ","), "\n")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 6: NATIONAL AVERAGES
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 6: National averages...\n")

natl <- dbGetQuery(con, sprintf("
  SELECT
    %s AS code,
    SUM(CAST(%s AS DOUBLE)) / NULLIF(%s, 0) AS national_avg,
    SUM(CAST(%s AS DOUBLE)) AS national_spend,
    %s AS national_claims,
    COUNT(DISTINCT state) AS n_states,
    %s AS n_providers
  FROM spending
  GROUP BY %s
  ORDER BY national_spend DESC
", col_hcpcs, col_pay, claims_expr, col_pay, claims_expr, npi_expr, col_hcpcs))

cat("  Codes:", nrow(natl), "\n")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 7: YEARLY TRENDS
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 7: Yearly trends...\n")

has_month <- !is.null(col_month)

if (has_month) {
  yearly_natl <- dbGetQuery(con, sprintf("
    SELECT
      CAST(LEFT(CAST(%s AS VARCHAR), 4) AS INTEGER) AS year,
      SUM(CAST(%s AS DOUBLE)) AS total_spend,
      %s AS total_claims,
      %s AS total_bene
    FROM spending
    GROUP BY year ORDER BY year
  ", col_month, col_pay, claims_expr, bene_expr))
  
  cat("  Years:", paste(yearly_natl$year, collapse = ", "), "\n")
  
  # Per-code yearly (top 500 by spend)
  top_codes_for_trend <- head(natl$code, 500)
  code_list <- paste0("'", top_codes_for_trend, "'", collapse = ",")
  
  yearly_codes <- dbGetQuery(con, sprintf("
    SELECT
      %s AS code,
      CAST(LEFT(CAST(%s AS VARCHAR), 4) AS INTEGER) AS year,
      SUM(CAST(%s AS DOUBLE)) / NULLIF(%s, 0) AS avg_rate,
      %s AS total_claims
    FROM spending
    WHERE %s IN (%s)
    GROUP BY code, year ORDER BY code, year
  ", col_hcpcs, col_month, col_pay, claims_expr, claims_expr,
     col_hcpcs, code_list))
  
  cat("  Code-level trends for top", length(top_codes_for_trend), "codes\n")
} else {
  cat("  No month column — skipping\n")
  yearly_natl <- NULL; yearly_codes <- NULL
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 8: PROVIDER CONCENTRATION (top 100)
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 8: Provider concentration...\n")

concentration <- list()
if (!is.null(col_npi)) {
  top_conc_codes <- head(natl$code, 100)
  
  # Process in batches of 25 to manage memory
  batch_size <- 25
  n_batches <- ceiling(length(top_conc_codes) / batch_size)
  
  for (b in 1:n_batches) {
    idx_start <- (b - 1) * batch_size + 1
    idx_end <- min(b * batch_size, length(top_conc_codes))
    batch_codes <- top_conc_codes[idx_start:idx_end]
    code_list <- paste0("'", batch_codes, "'", collapse = ",")
    
    cat(sprintf("  Batch %d/%d (codes %d-%d)...\n", b, n_batches, idx_start, idx_end))
    
    batch_data <- tryCatch(
      dbGetQuery(con, sprintf("
        SELECT %s AS code, %s AS npi, SUM(CAST(%s AS DOUBLE)) AS spend
        FROM spending
        WHERE %s IN (%s)
        GROUP BY code, npi
        ORDER BY code, spend DESC
      ", col_hcpcs, col_npi, col_pay, col_hcpcs, code_list)),
      error = function(e) { cat("  WARNING: batch failed:", e$message, "\n"); NULL }
    )
    
    if (is.null(batch_data) || nrow(batch_data) == 0) next
    
    for (code in batch_codes) {
      prov_dist <- batch_data[batch_data$code == code, ]
      n <- nrow(prov_dist)
      if (n < 10) next
      
      total <- sum(prov_dist$spend)
      top1  <- sum(prov_dist$spend[1:max(1, floor(n * 0.01))]) / total * 100
      top5  <- sum(prov_dist$spend[1:max(1, floor(n * 0.05))]) / total * 100
      top10 <- sum(prov_dist$spend[1:max(1, floor(n * 0.10))]) / total * 100
      
      sorted_spend <- sort(prov_dist$spend)
      ranks <- 1:n
      gini <- abs(1 - 2 * sum((n + 1 - ranks) * sorted_spend) / (n * total))
      
      concentration[[code]] <- list(
        top1_pct = round(top1, 1), top5_pct = round(top5, 1),
        top10_pct = round(top10, 1), gini = round(gini, 3),
        n_providers = n
      )
    }
    rm(batch_data); gc(verbose = FALSE)
  }
  cat("  Concentration computed for", length(concentration), "codes\n")
} else {
  cat("  No NPI column — skipping\n")
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 9: SUB-STATE REGIONS (ZIP3)
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 9: Sub-state regions...\n")

regions_data <- NULL
if (has_zip) {
  cat("  Aggregating by state × ZIP3...\n")
  
  # State × ZIP3 summary
  regions_data <- dbGetQuery(con, sprintf("
    SELECT
      state,
      zip3,
      SUM(CAST(%s AS DOUBLE)) AS total_paid,
      %s AS total_claims,
      %s AS n_providers,
      COUNT(DISTINCT %s) AS n_codes
    FROM spending
    WHERE zip3 IS NOT NULL
    GROUP BY state, zip3
    HAVING %s >= 3
    ORDER BY state, total_paid DESC
  ", col_pay, claims_expr, npi_expr, col_hcpcs, npi_expr))
  
  cat("  Regions:", nrow(regions_data), "state × ZIP3 combinations\n")
  
  # Top codes per region (top 5 states, top 10 ZIP3s each, top 20 codes)
  top_states <- head(unique(rate_by_state$code[1]), 5)
  
  # Region × code rates for top 50 codes
  top_50_codes <- head(natl$code, 50)
  code_list_50 <- paste0("'", top_50_codes, "'", collapse = ",")
  
  region_rates <- dbGetQuery(con, sprintf("
    SELECT
      state,
      zip3,
      %s AS code,
      SUM(CAST(%s AS DOUBLE)) / NULLIF(%s, 0) AS avg_rate,
      %s AS total_claims
    FROM spending
    WHERE zip3 IS NOT NULL AND %s IN (%s)
    GROUP BY state, zip3, %s
  ", col_hcpcs, col_pay, claims_expr, claims_expr, col_hcpcs, code_list_50, col_hcpcs))
  
  cat("  Region × code rates for top 50 codes:", nrow(region_rates), "rows\n")
} else {
  cat("  No ZIP data — skipping sub-state regions\n")
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 9.5: CASE MIX INDICES (Price vs Utilization decomposition)
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 9.5: Case mix indices...\n")

# Laspeyres decomposition: decomposes state spending differences into
# price effect (state pays different rates) vs mix effect (different service usage)
# Only use codes with both national rates and state data
case_mix_codes <- natl[!is.na(natl$national_avg) & natl$national_avg > 0 &
                       natl$national_claims > 0, ]

natl_total_claims <- sum(case_mix_codes$national_claims, na.rm = TRUE)
case_mix_codes$natl_share <- case_mix_codes$national_claims / natl_total_claims

# National benchmark: Σ(national_rate × national_claims_share)
natl_benchmark <- sum(case_mix_codes$national_avg * case_mix_codes$natl_share, na.rm = TRUE)

case_mix_results <- data.frame(state = character(0), price_index = numeric(0),
                                mix_index = numeric(0), interaction = numeric(0),
                                n_codes_used = integer(0), stringsAsFactors = FALSE)

for (st in unique(rate_by_state$state)) {
  st_rates <- rate_by_state[rate_by_state$state == st, ]
  # Merge with national data
  merged <- merge(case_mix_codes, st_rates[, c("code", "avg_rate", "total_claims")],
                  by = "code", all.x = TRUE, suffixes = c("_natl", "_st"))
  merged <- merged[!is.na(merged$avg_rate) & merged$avg_rate > 0, ]
  
  if (nrow(merged) < 20) next  # Skip states with too few codes
  
  st_total_claims <- sum(merged$total_claims, na.rm = TRUE)
  if (st_total_claims <= 0) next
  merged$st_share <- merged$total_claims / st_total_claims
  
  # Price index: state rates at national utilization mix
  price_idx <- sum(merged$avg_rate * merged$natl_share, na.rm = TRUE) / natl_benchmark
  
  # Mix index: national rates at state utilization mix
  mix_idx <- sum(merged$national_avg * merged$st_share, na.rm = TRUE) / natl_benchmark
  
  # Interaction (residual)
  actual <- sum(merged$avg_rate * merged$st_share, na.rm = TRUE) / natl_benchmark
  interaction <- actual - price_idx - mix_idx + 1
  
  case_mix_results <- rbind(case_mix_results, data.frame(
    state = st, price_index = round(price_idx, 4),
    mix_index = round(mix_idx, 4), interaction = round(interaction, 4),
    n_codes_used = nrow(merged), stringsAsFactors = FALSE
  ))
}

cat("  Case mix computed for", nrow(case_mix_results), "states\n")
if (nrow(case_mix_results) > 0) {
  cat("  Price index range:", round(min(case_mix_results$price_index), 3), "to",
      round(max(case_mix_results$price_index), 3), "\n")
  cat("  Mix index range:", round(min(case_mix_results$mix_index), 3), "to",
      round(max(case_mix_results$mix_index), 3), "\n")
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 10: STATE SUMMARIES
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 10: State summaries...\n")

state_summary <- dbGetQuery(con, sprintf("
  SELECT
    state,
    SUM(CAST(%s AS DOUBLE)) AS total_spend,
    %s AS total_claims,
    %s AS total_bene,
    %s AS n_providers,
    COUNT(DISTINCT %s) AS n_codes
  FROM spending
  GROUP BY state ORDER BY total_spend DESC
", col_pay, claims_expr, bene_expr, npi_expr, col_hcpcs))

state_summary <- merge(state_summary, FMAP, by = "state", all.x = TRUE)

# Merge case mix indices
if (nrow(case_mix_results) > 0) {
  state_summary <- merge(state_summary, case_mix_results, by = "state", all.x = TRUE)
}

# Real Medicaid enrollment by state (CMS Nov 2024, Medicaid only, not CHIP)
# Source: CMS Medicaid & CHIP Monthly Enrollment via KFF/World Population Review
ENROLLMENT <- c(
  AL=771880, AK=234784, AZ=1810967, AR=736281, CA=12175605,
  CO=1060589, CT=905755, DE=236760, DC=238822, FL=3624248,
  GA=1700970, HI=380931, ID=295105, IL=2936525, IN=1626670,
  IA=586582, KS=335093, KY=1251466, LA=1362390, ME=320095,
  MD=1312635, MA=1455521, MI=2188259, MN=1152907, MS=516505,
  MO=1127066, MT=196154, NE=302679, NV=710223, NH=165705,
  NJ=1509804, NM=699086, NY=5952946, NC=2451104, ND=99978,
  OH=2632166, OK=904003, OR=1119407, PA=2781015, RI=273400,
  SC=936778, SD=126747, TN=1269667, TX=3833095, UT=301924,
  VT=151464, VA=1605016, WA=1776840, WV=468995, WI=1109236,
  WY=57107
)

state_summary$est_enrollment <- ENROLLMENT[state_summary$state]
# States not in lookup fall back to proportional estimate
missing <- is.na(state_summary$est_enrollment)
if (any(missing)) {
  total_known <- sum(state_summary$total_spend[!missing])
  total_enroll_known <- sum(ENROLLMENT[state_summary$state[!missing]], na.rm = TRUE)
  state_summary$est_enrollment[missing] <- state_summary$total_spend[missing] /
    total_known * total_enroll_known
  cat("  Enrollment: real data for", sum(!missing), "states, estimated for", sum(missing), "\n")
} else {
  cat("  Enrollment: real CMS data for all", nrow(state_summary), "states\n")
}
state_summary$per_enrollee <- state_summary$total_spend / state_summary$est_enrollment

# Provider counts by category
if (!is.null(col_npi)) {
  prov_cats <- dbGetQuery(con, sprintf("
    SELECT state,
      COUNT(DISTINCT CASE WHEN %s IN ('99211','99212','99213','99214','99215',
        '99201','99202','99203','99204','99205') THEN %s END) AS em_provs,
      COUNT(DISTINCT CASE WHEN %s IN ('T1019','T2025','T1020','S5125','S5130',
        'T2003','T1005') THEN %s END) AS hcbs_provs,
      COUNT(DISTINCT CASE WHEN %s IN ('97153','90834','90837','H0015','H2019',
        '90832','90847','96130') THEN %s END) AS bh_provs,
      COUNT(DISTINCT CASE WHEN LEFT(%s,1) = 'D' THEN %s END) AS dental_provs
    FROM spending GROUP BY state
  ", col_hcpcs, col_npi, col_hcpcs, col_npi, col_hcpcs, col_npi,
     col_hcpcs, col_npi))
  
  state_summary <- merge(state_summary, prov_cats, by = "state", all.x = TRUE)
}

cat("  States:", nrow(state_summary), "\n")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 10.5: PROVIDER PROFILES (top 200/state, trends, peer comparison)
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 10.5: Provider profiles...\n")

providers_json <- NULL
if (!is.null(col_npi)) {
  # Get top 200 providers per state by total spend
  top_provs <- dbGetQuery(con, sprintf("
    SELECT
      state,
      %s AS npi,
      SUM(CAST(%s AS DOUBLE)) AS total_paid,
      %s AS total_claims,
      %s AS total_bene,
      COUNT(DISTINCT %s) AS n_codes,
      ROW_NUMBER() OVER (PARTITION BY state ORDER BY SUM(CAST(%s AS DOUBLE)) DESC) AS rn
    FROM spending
    GROUP BY state, %s
  ", col_npi, col_pay, claims_expr, bene_expr, col_hcpcs, col_pay, col_npi))
  
  top_provs <- top_provs[top_provs$rn <= 200, ]
  top_provs$rn <- NULL
  cat("  Top providers:", nrow(top_provs), "across", length(unique(top_provs$state)), "states\n")
  
  # Get NPPES info (name, taxonomy, entity type) if available
  has_nppes_view <- tryCatch({
    dbGetQuery(con, "SELECT 1 FROM nppes_raw LIMIT 1"); TRUE
  }, error = function(e) FALSE)
  
  if (has_nppes_view) {
    np_cols <- dbGetQuery(con, "SELECT column_name FROM information_schema.columns WHERE table_name = 'nppes_raw'")$column_name
    
    np_npi   <- detect_col(c("NPI","npi"), np_cols)
    np_first <- detect_col(c("Provider First Name","provider_first_name",
                              "PROVIDER_FIRST_NAME"), np_cols)
    np_last  <- detect_col(c("Provider Last Name (Legal Name)","provider_last_name_legal_name",
                              "PROVIDER_LAST_NAME_LEGAL_NAME","Provider Last Name"), np_cols)
    np_org   <- detect_col(c("Provider Organization Name (Legal Business Name)",
                              "provider_organization_name_legal_business_name",
                              "PROVIDER_ORGANIZATION_NAME"), np_cols)
    np_tax   <- detect_col(c("Healthcare Provider Taxonomy Code_1",
                              "healthcare_provider_taxonomy_code_1",
                              "HEALTHCARE_PROVIDER_TAXONOMY_CODE_1"), np_cols)
    np_etype <- detect_col(c("Entity Type Code","entity_type_code"), np_cols)
    
    if (!is.null(np_npi)) {
      info_cols <- sprintf("CAST(\"%s\" AS VARCHAR) AS npi", np_npi)
      if (!is.null(np_org)) info_cols <- c(info_cols, sprintf("\"%s\" AS org_name", np_org))
      if (!is.null(np_first)) info_cols <- c(info_cols, sprintf("\"%s\" AS first_name", np_first))
      if (!is.null(np_last)) info_cols <- c(info_cols, sprintf("\"%s\" AS last_name", np_last))
      if (!is.null(np_tax)) info_cols <- c(info_cols, sprintf("\"%s\" AS taxonomy", np_tax))
      if (!is.null(np_etype)) info_cols <- c(info_cols, sprintf("\"%s\" AS entity_type", np_etype))
      
      npi_list <- paste0("'", unique(top_provs$npi), "'", collapse = ",")
      
      nppes_info <- dbGetQuery(con, sprintf("
        SELECT %s FROM nppes_raw WHERE CAST(\"%s\" AS VARCHAR) IN (%s)
      ", paste(info_cols, collapse = ", "), np_npi, npi_list))
      
      if (nrow(nppes_info) > 0) {
        top_provs <- merge(top_provs, nppes_info, by = "npi", all.x = TRUE)
        cat("  NPPES matched:", sum(!is.na(top_provs$last_name) | !is.na(top_provs$org_name)),
            "of", nrow(top_provs), "providers\n")
      }
    }
  }
  
  # Yearly trends per provider (if month column exists)
  prov_trends <- NULL
  if (has_month) {
    cat("  Computing provider yearly trends...\n")
    npi_list_all <- paste0("'", unique(top_provs$npi), "'", collapse = ",")
    prov_trends <- tryCatch(
      dbGetQuery(con, sprintf("
        SELECT
          %s AS npi,
          CAST(LEFT(CAST(%s AS VARCHAR), 4) AS INTEGER) AS year,
          SUM(CAST(%s AS DOUBLE)) AS yr_paid,
          %s AS yr_claims
        FROM spending
        WHERE %s IN (%s)
        GROUP BY npi, year ORDER BY npi, year
      ", col_npi, col_month, col_pay, claims_expr, col_npi, npi_list_all)),
      error = function(e) { cat("  WARNING: provider trends query failed:", e$message, "\n"); NULL }
    )
    if (!is.null(prov_trends)) cat("  Provider yearly trends:", nrow(prov_trends), "provider-years\n")
  }
  gc(verbose = FALSE)
  
  # Case mix per provider (top 10 codes by spend)
  cat("  Computing provider case mix...\n")
  npi_list_all <- paste0("'", unique(top_provs$npi), "'", collapse = ",")
  prov_codes <- tryCatch(
    dbGetQuery(con, sprintf("
      SELECT
        %s AS npi,
        %s AS code,
        SUM(CAST(%s AS DOUBLE)) AS code_paid,
        %s AS code_claims
      FROM spending
      WHERE %s IN (%s)
      GROUP BY npi, code
      ORDER BY npi, code_paid DESC
    ", col_npi, col_hcpcs, col_pay, claims_expr, col_npi, npi_list_all)),
    error = function(e) { cat("  WARNING: provider case mix query failed:", e$message, "\n"); NULL }
  )
  
  if (!is.null(prov_codes)) cat("  Provider x code pairs:", format(nrow(prov_codes), big.mark = ","), "\n")
  
  # Compute peer benchmarks by taxonomy × state
  peer_benchmarks <- NULL
  if ("taxonomy" %in% names(top_provs)) {
    tax_counts <- table(top_provs$taxonomy[!is.na(top_provs$taxonomy)])
    common_tax <- names(tax_counts[tax_counts >= 5])
    if (length(common_tax) > 0) {
      peer_sub <- top_provs[!is.na(top_provs$taxonomy) & top_provs$taxonomy %in% common_tax, ]
      pb_agg <- aggregate(
        cbind(total_paid, total_claims) ~ taxonomy + state,
        data = peer_sub,
        FUN = median
      )
      names(pb_agg)[3:4] <- c("peer_med_paid", "peer_med_claims")
      pb_n <- aggregate(npi ~ taxonomy + state, data = peer_sub, FUN = length)
      names(pb_n)[3] <- "peer_count"
      peer_benchmarks <- merge(pb_agg, pb_n, by = c("taxonomy", "state"))
      cat("  Peer benchmarks:", nrow(peer_benchmarks), "taxonomy x state combos\n")
    }
  }
  
  # Build provider case mix
  prov_code_list <- if (!is.null(prov_codes)) split(prov_codes, prov_codes$npi) else list()
  prov_trend_list <- if (!is.null(prov_trends)) split(prov_trends, prov_trends$npi) else list()
  
  prov_case_mix <- lapply(names(prov_code_list), function(npi) {
    pc <- prov_code_list[[npi]]
    total <- sum(pc$code_paid, na.rm = TRUE)
    if (total <= 0) return(NULL)
    
    pc$cat <- sapply(pc$code, hcpcs_category)
    cat_shares <- tapply(pc$code_paid, pc$cat, sum, na.rm = TRUE) / total
    cat_shares <- sort(cat_shares, decreasing = TRUE)
    
    top10 <- head(pc, 10)
    top_codes <- lapply(1:nrow(top10), function(i) {
      list(code = top10$code[i], desc = get_desc(top10$code[i]),
           paid = round(top10$code_paid[i], 0),
           claims = top10$code_claims[i],
           share = round(top10$code_paid[i] / total * 100, 1))
    })
    
    list(npi = npi,
         category_shares = as.list(round(cat_shares * 100, 1)),
         top_codes = top_codes)
  })
  names(prov_case_mix) <- names(prov_code_list)
  
  # Assemble final provider list
  providers_list <- lapply(1:nrow(top_provs), function(i) {
    p <- top_provs[i, ]
    cm <- prov_case_mix[[as.character(p$npi)]]
    
    entry <- list(
      npi = p$npi, state = p$state,
      total_paid = round(p$total_paid, 0),
      total_claims = p$total_claims,
      total_bene = p$total_bene,
      n_codes = p$n_codes
    )
    
    # Provider identity
    if ("entity_type" %in% names(p) && !is.na(p$entity_type) && p$entity_type == "2") {
      entry$name <- if (!is.na(p$org_name)) p$org_name else "Organization"
      entry$type <- "org"
    } else {
      first <- if ("first_name" %in% names(p) && !is.na(p$first_name)) p$first_name else ""
      last <- if ("last_name" %in% names(p) && !is.na(p$last_name)) p$last_name else ""
      entry$name <- trimws(paste(first, last))
      if (entry$name == "") entry$name <- paste("NPI", p$npi)
      entry$type <- "ind"
    }
    
    if ("taxonomy" %in% names(p) && !is.na(p$taxonomy)) entry$taxonomy <- p$taxonomy
    
    # Yearly trends
    pt <- prov_trend_list[[as.character(p$npi)]]
    if (!is.null(pt) && nrow(pt) > 1) {
      entry$trend <- lapply(1:nrow(pt), function(j) {
        list(y = pt$year[j], paid = round(pt$yr_paid[j], 0), claims = pt$yr_claims[j])
      })
    }
    
    # Peer comparison
    if (!is.null(peer_benchmarks) && "taxonomy" %in% names(p) && !is.na(p$taxonomy)) {
      pb <- peer_benchmarks[peer_benchmarks$taxonomy == p$taxonomy & peer_benchmarks$state == p$state, ]
      if (nrow(pb) > 0) {
        entry$peer <- list(
          med_paid = round(pb$peer_med_paid[1], 0),
          med_claims = round(pb$peer_med_claims[1], 0),
          n_peers = pb$peer_count[1],
          vs_med = round((p$total_paid / pb$peer_med_paid[1] - 1) * 100, 1)
        )
      }
    }
    
    # Case mix
    if (!is.null(cm)) {
      entry$category_shares <- cm$category_shares
      entry$top_codes <- cm$top_codes
    }
    
    entry
  })
  
  providers_json <- providers_list
  cat("  Provider profiles built:", length(providers_json), "\n")
} else {
  cat("  No NPI column — skipping provider profiles\n")
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 10.6: SPECIALTY AGGREGATION (taxonomy × state)
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 10.6: Specialty aggregation...\n")

specialties_json <- NULL
if (!is.null(col_npi) && has_nppes_view) {
  gc(verbose = FALSE)
  cat("  Running taxonomy × state aggregation...\n")
  # Join spending with taxonomy at the query level
  spec_data <- tryCatch(
    dbGetQuery(con, sprintf("
      SELECT
        g.taxonomy,
        s.state,
        COUNT(DISTINCT s.%s) AS n_providers,
        SUM(CAST(s.%s AS DOUBLE)) AS total_paid,
        %s AS total_claims,
        %s AS total_bene,
        COUNT(DISTINCT s.%s) AS n_codes,
        SUM(CAST(s.%s AS DOUBLE)) / NULLIF(COUNT(DISTINCT s.%s), 0) AS avg_paid_per_provider,
        SUM(CAST(s.%s AS DOUBLE)) / NULLIF(%s, 0) AS avg_rate_per_claim
      FROM spending s
      JOIN (
        SELECT DISTINCT CAST(\"%s\" AS VARCHAR) AS npi, \"%s\" AS taxonomy
        FROM nppes_raw
        WHERE \"%s\" IS NOT NULL AND \"%s\" IS NOT NULL
      ) g ON CAST(s.%s AS VARCHAR) = g.npi
      WHERE g.taxonomy IS NOT NULL AND g.taxonomy != ''
      GROUP BY g.taxonomy, s.state
      HAVING COUNT(DISTINCT s.%s) >= 3
      ORDER BY total_paid DESC
    ", col_npi, col_pay, claims_expr, bene_expr, col_hcpcs,
       col_pay, col_npi, col_pay, claims_expr,
       np_npi, np_tax, np_npi, np_tax, col_npi, col_npi)),
    error = function(e) { cat("  WARNING: specialty query failed:", e$message, "\n"); NULL }
  )
  
  if (!is.null(spec_data)) cat("  Specialty x state combos:", nrow(spec_data), "\n")
  
  if (!is.null(spec_data) && nrow(spec_data) > 0) {
    # Get national totals per taxonomy
    spec_natl <- aggregate(cbind(total_paid, total_claims, n_providers) ~ taxonomy,
                            data = spec_data, FUN = sum)
    # Keep top 200 specialties by total spend
    spec_natl <- spec_natl[order(-spec_natl$total_paid), ]
    top_spec <- head(spec_natl$taxonomy, 200)
    spec_data <- spec_data[spec_data$taxonomy %in% top_spec, ]
    
    # Build JSON: list of specialties, each with per-state breakdown
    spec_list <- lapply(top_spec, function(tax) {
      rows <- spec_data[spec_data$taxonomy == tax, ]
      natl_row <- spec_natl[spec_natl$taxonomy == tax, ]
      by_state <- lapply(1:nrow(rows), function(j) {
        list(state = rows$state[j], provs = rows$n_providers[j],
             paid = round(rows$total_paid[j], 0),
             claims = rows$total_claims[j],
             avg_per_prov = round(rows$avg_paid_per_provider[j], 0),
             avg_per_claim = round(rows$avg_rate_per_claim[j], 2))
      })
      list(
        taxonomy = tax,
        national_paid = round(natl_row$total_paid[1], 0),
        national_providers = natl_row$n_providers[1],
        national_claims = natl_row$total_claims[1],
        n_states = nrow(rows),
        states = by_state
      )
    })
    
    specialties_json <- spec_list
    cat("  Specialties profiled:", length(specialties_json), "\n")
  }
} else {
  cat("  No NPPES/taxonomy — skipping specialties\n")
}

# ═══════════════════════════════════════════════════════════════════════════
# STEP 11: EXPORT JSON
# ═══════════════════════════════════════════════════════════════════════════
cat("\nStep 11: Exporting JSON...\n")

# 1. states.json
write_json(state_summary, file.path(OUTPUT_DIR, "states.json"),
           pretty = TRUE, auto_unbox = TRUE)
cat("  → states.json (", nrow(state_summary), "states)\n")

# 2. hcpcs.json — ALL codes
hcpcs_list <- lapply(1:nrow(natl), function(i) {
  code <- natl$code[i]
  
  code_rates <- rate_by_state[rate_by_state$code == code, ]
  rates <- setNames(round(code_rates$avg_rate, 2), code_rates$state)
  rates <- as.list(rates[!is.na(rates)])
  
  desc_txt <- get_desc(code)
  
  entry <- list(
    code = code,
    desc = if (!is.na(desc_txt)) desc_txt else paste("HCPCS", code),
    category = hcpcs_category(code),
    national_avg = round(natl$national_avg[i], 2),
    national_claims = natl$national_claims[i],
    national_spend = round(natl$national_spend[i], 0),
    n_states = natl$n_states[i],
    n_providers = natl$n_providers[i],
    rates = rates
  )
  
  if (code %in% names(concentration)) {
    entry$concentration <- concentration[[code]]
  }
  
  if (!is.null(yearly_codes)) {
    ct <- yearly_codes[yearly_codes$code == code, ]
    if (nrow(ct) > 1) {
      entry$trend <- lapply(1:nrow(ct), function(j) {
        list(year = ct$year[j], avg_rate = round(ct$avg_rate[j], 2),
             claims = ct$total_claims[j])
      })
    }
  }
  
  entry
})

write_json(hcpcs_list, file.path(OUTPUT_DIR, "hcpcs.json"),
           pretty = TRUE, auto_unbox = TRUE)
cat("  → hcpcs.json (", length(hcpcs_list), "codes)\n")

# 3. trends.json
if (!is.null(yearly_natl)) {
  write_json(yearly_natl, file.path(OUTPUT_DIR, "trends.json"),
             pretty = TRUE, auto_unbox = TRUE)
  cat("  → trends.json (", nrow(yearly_natl), "years)\n")
}

# 4. regions.json — sub-state geography
if (!is.null(regions_data)) {
  # Add region names
  regions_data$region_name <- mapply(get_zip3_name,
    regions_data$zip3, regions_data$state)
  
  # Disambiguate duplicate names within same state
  for (st in unique(regions_data$state)) {
    idx <- which(regions_data$state == st)
    nms <- regions_data$region_name[idx]
    dups <- nms %in% nms[duplicated(nms)]
    regions_data$region_name[idx[dups]] <- paste0(
      regions_data$region_name[idx[dups]], " (", regions_data$zip3[idx[dups]], ")")
  }
  
  # Build nested structure: { state: { zip3: { name, spend, claims, ... } } }
  region_output <- list(
    summary = split(
      regions_data[, c("zip3","region_name","total_paid","total_claims",
                       "n_providers","n_codes")],
      regions_data$state
    )
  )
  
  # Add region × code rates
  if (exists("region_rates") && nrow(region_rates) > 0) {
    region_rates_nested <- list()
    for (st in unique(region_rates$state)) {
      st_data <- region_rates[region_rates$state == st, ]
      region_rates_nested[[st]] <- split(
        st_data[, c("zip3","code","avg_rate","total_claims")],
        st_data$zip3
      )
    }
    region_output$rates <- region_rates_nested
  }
  
  write_json(region_output, file.path(OUTPUT_DIR, "regions.json"),
             pretty = TRUE, auto_unbox = TRUE)
  cat("  → regions.json (", nrow(regions_data), "regions)\n")
}

# 5. providers.json
if (!is.null(providers_json) && length(providers_json) > 0) {
  write_json(providers_json, file.path(OUTPUT_DIR, "providers.json"),
             pretty = TRUE, auto_unbox = TRUE)
  cat("  → providers.json (", length(providers_json), "providers)\n")
}

# 6. specialties.json
if (!is.null(specialties_json) && length(specialties_json) > 0) {
  write_json(specialties_json, file.path(OUTPUT_DIR, "specialties.json"),
             pretty = TRUE, auto_unbox = TRUE)
  cat("  → specialties.json (", length(specialties_json), "specialties)\n")
}

# 7. meta.json
meta <- list(
  generated = as.character(Sys.time()),
  source = "T-MSIS",
  source_file = basename(SPENDING_FILE),
  provider_file = if (has_providers) basename(PROVIDER_FILE) else NULL,
  nppes_file = if (has_nppes) basename(NPPES_FILE) else NULL,
  source_rows = row_count,
  matched_rows = matched,
  n_states = nrow(state_summary),
  n_codes = length(hcpcs_list),
  n_codes_with_desc = sum(sapply(hcpcs_list, function(x) x$desc != paste("HCPCS", x$code))),
  n_codes_with_trends = if (!is.null(yearly_codes)) length(unique(yearly_codes$code)) else 0,
  n_codes_with_concentration = length(concentration),
  has_regions = !is.null(regions_data),
  n_regions = if (!is.null(regions_data)) nrow(regions_data) else 0,
  has_case_mix = nrow(case_mix_results) > 0,
  has_providers = !is.null(providers_json) && length(providers_json) > 0,
  n_provider_profiles = if (!is.null(providers_json)) length(providers_json) else 0,
  has_specialties = !is.null(specialties_json) && length(specialties_json) > 0,
  n_specialties = if (!is.null(specialties_json)) length(specialties_json) else 0,
  years = if (!is.null(yearly_natl)) sort(yearly_natl$year) else NULL,
  total_spend = sum(state_summary$total_spend),
  live = TRUE,
  engine = "duckdb",
  version = "0.5.0"
)
write_json(meta, file.path(OUTPUT_DIR, "meta.json"),
           pretty = TRUE, auto_unbox = TRUE)
cat("  → meta.json\n")

# File sizes
files <- list.files(OUTPUT_DIR, full.names = TRUE, pattern = "\\.json$")
sizes <- file.size(files) / 1024 / 1024
cat("\n  Output sizes:\n")
for (i in seq_along(files)) {
  cat(sprintf("    %s: %.1f MB\n", basename(files[i]), sizes[i]))
}
cat(sprintf("    Total: %.1f MB\n", sum(sizes)))

# ── Cleanup ──────────────────────────────────────────────────────────────
dbDisconnect(con, shutdown = TRUE)

# ── Summary ──────────────────────────────────────────────────────────────
cat("\n═══════════════════════════════════════════════════\n")
cat("  Pipeline complete!\n\n")
cat("  Output:        ", OUTPUT_DIR, "\n")
cat("  States:        ", nrow(state_summary), "\n")
cat("  HCPCS codes:   ", length(hcpcs_list), "(every code in the dataset)\n")
cat("  Descriptions:  ", meta$n_codes_with_desc, "of", length(hcpcs_list), "codes named\n")
cat("  Trends:        ", meta$n_codes_with_trends, "codes × ",
    if (!is.null(yearly_natl)) nrow(yearly_natl) else 0, "years\n")
cat("  Concentration: ", length(concentration), "codes\n")
cat("  Regions:       ", if (!is.null(regions_data)) nrow(regions_data) else "none", "\n")
cat("  Case mix:      ", nrow(case_mix_results), "states with price/mix indices\n")
cat("  Providers:     ", if (!is.null(providers_json)) length(providers_json) else "none", "\n")
cat("  Specialties:   ", if (!is.null(specialties_json)) length(specialties_json) else "none", "\n")
cat("  Total spend:   $", format(sum(state_summary$total_spend) / 1e9, digits = 4), "B\n")
cat("═══════════════════════════════════════════════════\n\n")
cat("  Next steps:\n")
cat("  1. cd .. && npm run build\n")
cat("  2. npm run preview          (check at localhost:4173)\n")
cat("  3. vercel --prod            (deploy)\n")
if (!has_nppes) {
  cat("\n  For sub-state regions, download NPPES and re-run:\n")
  cat("  https://download.cms.gov/nppes/NPI_Files.html\n")
}
cat("\n")
