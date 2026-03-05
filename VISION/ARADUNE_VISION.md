# T-MSIS resource library: verified URLs for data quality, documentation, and the DOGE dataset

**This is a comprehensive, verified URL reference for the Transformed Medicaid Statistical Information System (T-MSIS) ecosystem** — covering CMS official documentation, ResDAC technical guides, research organization publications, GitHub repositories, the February 2026 HHS/DOGE Medicaid Provider Spending dataset, and academic papers. Every URL below was located through web search and verified via direct page fetch as of March 4, 2026. Where a resource is offline or returns an access error, this is noted explicitly.

---

## 1. CMS official documentation on Medicaid.gov

### Core T-MSIS pages

| URL | Title | Notes |
|-----|-------|-------|
| https://www.medicaid.gov/medicaid/data-systems/medicaid-and-chip-business-information-solution/transformed-medicaid-statistical-information-system-t-msis | **T-MSIS Main Landing Page** | Primary overview of T-MSIS including the OBA (Outcomes Based Assessment) data quality map, file submission status, and links to the Data Guide, TAF, DQ Atlas, and Scorecard. Updated monthly. |
| https://www.medicaid.gov/medicaid/data-systems/macbis/medicaid-chip-research-files/transformed-medicaid-statistical-information-system-t-msis-analytic-files-taf | **T-MSIS Analytic Files (TAF) Landing Page** | Primary page for TAF RIF descriptions, DQ Atlas link, Behavioral Health and SUD Data Books, per capita expenditure methodology, pregnancy identification algorithm, and TAF RIF availability chart. |
| https://www.medicaid.gov/tmsis/dataguide/ | **T-MSIS Data Guide (Interactive)** | Web-based tool for navigating the T-MSIS data dictionary, validation rules, data quality measures, record layout mapping requirements, and technical instructions. |
| https://www.medicaid.gov/tmsis/dataguide/v4/ | **T-MSIS Data Guide V4.0** | Version 4.0 of the Data Guide reflecting major file layout changes. CMS set a September 2025 deadline for states to submit in V4.0 format. Includes file segment layouts, data element specs, validation rules, and appendices. |
| https://www.medicaid.gov/medicaid/data-systems/medicaid-and-chip-business-information-solution/transformed-medicaid-statistical-information-system-t-msis/t-msis-data-guide | **T-MSIS Data Guide Overview** | Overview page describing the Data Guide and how it works in conjunction with the Technical Instructions (formerly Coding Blog). |

### Technical instructions (formerly T-MSIS Coding Blog)

| URL | Title | Notes |
|-----|-------|-------|
| https://www.medicaid.gov/tmsis/dataguide/t-msis-coding-blog/ | **Technical Instructions Hub** | Filterable listing of all technical instructions organized by category (Claims, Eligibility, File Creation, Managed Care, Provider, Special Programs) and file type. The former Coding Blog was integrated here. |
| https://www.medicaid.gov/tmsis/dataguide/t-msis-coding-blog/cms-guidance-reporting-provider-identifiers-in-t-msis/ | **CMS Guidance: Reporting Provider Identifiers in T-MSIS** | Defines each provider identifier type (NPI, Medicare ID, NCPDP, FTIN, State Tax ID, SSN) and validation requirements. Emphasizes NPPES-issued NPIs for all HIPAA-covered providers. |
| https://www.medicaid.gov/tmsis/dataguide/t-msis-coding-blog/cms-guidance-reporting-provider-facility-group-individual-code-in-t-msis/ | **CMS Guidance: Provider Facility-Group-Individual-Code** | Guidance on uniquely identifying providers at NPI/FTIN level, FGI code values, and cross-state consistency best practices. |
| https://www.medicaid.gov/tmsis/dataguide/t-msis-coding-blog/cms-technical-instructions-provider-classification-requirements-in-tmsis/ | **Provider Classification Requirements** | Technical instructions on provider taxonomy/specialization reporting. CMS identifies NUCC taxonomy codes as the preferred method. |
| https://www.medicaid.gov/tmsis/dataguide/t-msis-coding-blog/submitting-accurate-and-complete-encounter-data-managed-care/ | **Encounter Data Reporting in T-MSIS** | Guidance on CMS expectations for complete and accurate managed care encounter data, including ACA requirements. |
| https://www.medicaid.gov/tmsis/dataguide/t-msis-coding-blog/cms-technical-instructions-reporting-personal-care-and-home-health-services-in-t-msis/ | **Personal Care and Home Health Services Reporting** | Addresses EVV data mapping, NPI requirements for PCS providers, and procedure code guidance. |
| https://www.medicaid.gov/medicaid/data-and-systems/macbis/tmsis/tmsis-blog/136886 | **Reporting Amounts Paid to Providers** | Describes T-MSIS data elements for payments to providers on FFS claims and managed care encounters. |

### CMS newsroom fact sheets and press releases

| URL | Title | Notes |
|-----|-------|-------|
| https://www.cms.gov/newsroom/fact-sheets/medicaid-and-chip-t-msis-analytic-files-data-release | **TAF Data Release Fact Sheet (Nov 2019)** | Original fact sheet announcing first TAF RIFs for CY 2014–2016, with details on 35 DQ briefs and technical guidance documents. |
| https://www.cms.gov/newsroom/fact-sheets/fact-sheet-medicaid-and-chip-t-msis-analytic-files-data-release | **TAF Data Release Fact Sheet (2020 Refresh)** | Updated fact sheet covering refreshed 2014–2016 TAF RIFs, DQ Atlas expansion, and the 32 T-MSIS Priority Items (TPIs). |
| https://www.cms.gov/newsroom/fact-sheets/additional-medicaid-and-chip-t-msis-analytic-files-data-release | **TAF Data Release Fact Sheet (Sep 2020, CY2017–2018)** | Fact sheet for CY 2017–2018 TAF RIF release noting significant DQ improvement over prior years. |
| https://www.cms.gov/newsroom/press-releases/cms-releases-updates-medicaid-and-chip-transformed-medicaid-statistical-information-system-t-msis | **CMS Press Release: T-MSIS Data Updates (2020)** | Press release announcing updated T-MSIS data files with improved data quality. |
| https://www.cms.gov/newsroom/press-releases/trump-administration-prioritizes-affordability-announcing-major-crackdown-health-care-fraud | **CMS Press Release: Healthcare Fraud Crackdown (Feb 25, 2026)** | Announces $259.5M Minnesota Medicaid deferral, DMEPOS enrollment moratorium, and CRUSH RFI with March 20, 2026 comment deadline. |

### Policy guidance and methodology documents

| URL | Title | Notes |
|-----|-------|-------|
| https://www.medicaid.gov/federal-policy-guidance/downloads/sho25002.pdf | **SHO #25-002: T-MSIS Data Reporting Compliance** | May 28, 2025 letter reaffirming T-MSIS DQ expectations. Formally rescinds TPIs and replaces them with OBA framework. Covers V4.0 compliance, MES processes, and September 2025 DQ routine compliance deadline. |
| https://www.medicaid.gov/state-overviews/scorecard/content/scorecard-release/PerCapitaExpendDataMethod-2025.pdf | **Per Capita Expenditure Methodology (2025 Scorecard)** | Detailed methodology for calculating state-level per capita Medicaid expenditures using TAF and CMS-64 data across five eligibility groups. |
| https://www.medicaid.gov/state-overviews/scorecard/measure/Medicaid-Per-Capita-Expenditures | **Interactive Scorecard: Per Capita Expenditures** | Interactive visualization of state-level per capita expenditure data from the Medicaid & CHIP Scorecard. |
| https://www.medicaid.gov/federal-policy-guidance/downloads/cib031819.pdf | **CIB: T-MSIS State Compliance (2019)** | CMCS Information Bulletin on T-MSIS state compliance requirements. |

### Encounter data toolkits

| URL | Title | Notes |
|-----|-------|-------|
| https://www.medicaid.gov/medicaid/downloads/medicaid-encounter-data-toolkit.pdf | **Encounter Data Toolkit (Mathematica, Nov 2013)** | Practical guide covering six building blocks for collecting, validating, and reporting managed care encounter data. |
| https://www.medicaid.gov/medicaid/downloads/ed-validation-toolkit.pdf | **State Toolkit for Validating Encounter Data (Aug 2019)** | Updated toolkit for encounter data validation including periodic audit process guidance per 42 CFR §438.602(e). |
| https://www.medicaid.gov/sites/default/files/2023-10/mmce-data-valdtn-tolkit.pdf | **Encounter Data Validation & Audit Toolkit (2023)** | Most recent edition of the state validation toolkit for managed care encounter data. |
| https://www.medicaid.gov/medicaid/managed-care/guidance/encounter-data | **Encounter Data Landing Page** | Links to both toolkits plus managed care regulation requirements. |

---

## 2. DQ Atlas resources and supplemental briefs

The **DQ Atlas** is CMS's interactive, web-based tool showing state-level data quality assessments for the TAF across **80+ topics grouped into nine thematic areas**. The supplemental briefs and background-and-methods documents below provide detailed methodology and analysis.

### Main page and supplemental briefs

| URL | Title | Notes |
|-----|-------|-------|
| https://www.medicaid.gov/dq-atlas/welcome | **DQ Atlas Main Page** | Landing page for the interactive DQ Atlas tool. |
| https://www.medicaid.gov/dq-atlas/downloads/supplemental/3051-TAF-Data-Run-Out.pdf | **Brief #3051: TAF Data Run-Out Timing (June 2025)** | Key methodology brief explaining run-out periods: enrollment records stabilize quickly; service use records need ≥6 months (preliminary) or ≥12 months (final) run-out. Managed care encounters require more time than FFS claims. |
| https://www.medicaid.gov/dq-atlas/downloads/supplemental/9010-Production-of-TAF-RIF.pdf | **Brief #9010: Production of TAF RIF (June 2021)** | Describes key transformations from state-submitted T-MSIS data to TAF, including PII removal and unique beneficiary ID assignment. |
| https://www.medicaid.gov/dq-atlas/downloads/supplemental/9020-TAF-CMS-64-Comparison.pdf | **Brief #9020: TAF and CMS-64 Expenditure Comparison (Dec 2021)** | Compares the two principal Medicaid spending data sources — TAF (claim-level, service-date based) and CMS-64 (aggregate, payment-date based) — and explains alignment methods. |
| https://www.medicaid.gov/dq-atlas/downloads/supplemental/5241-Federally-Assigned-Service-Category.pdf | **Brief #5241: Federally Assigned Service Category (FASC)** | Methodology for the FASC variable assigning all header-level claims in IP, LT, OT, and RX to 21 distinct service categories for cross-state analysis. |
| https://www.medicaid.gov/dq-atlas/downloads/supplemental/7061-Identifying-HCBS-in-TAF.pdf | **Brief #7061: Identifying HCBS in TAF** | How to identify 13 HCBS categories using procedure codes, revenue codes, type of bill, and other variables; distinguishes HCBS programs from State Plan benefits. |
| https://www.medicaid.gov/dq-atlas/downloads/supplemental/9081-School-Based-Providers-in-TAF.pdf | **Brief #9081: School-Based Providers in TAF (2025)** | Analyzes how school-based services are identified in TAF using diagnosis codes, procedure codes, place of service, and provider taxonomy codes. |
| https://www.medicaid.gov/dq-atlas/downloads/supplemental/3011_Final_Action_Status.pdf | **Brief #3011: Final Action Status in T-MSIS Claims** | Documents how T-MSIS claims are consolidated to identify final-action records, removing voids, duplicates, and fully denied claims. |

### Background and methods documents

| URL | Title | Notes |
|-----|-------|-------|
| https://www.medicaid.gov/dq-atlas/downloads/background-and-methods/TAF-DQ-Diagnosis-Cd-OT.pdf | **DQ Atlas B&M: Diagnosis Code — OT** | Explains DQ Atlas methodology for assessing diagnosis code quality on OT header records, with state classification criteria. |
| https://www.medicaid.gov/dq-atlas/downloads/background-and-methods/TAF-DQ-Diagnosis-Cd-LT.pdf | **DQ Atlas B&M: Diagnosis Code — LT** | Companion document for diagnosis code quality in Long-Term Care claims. |
| https://www.medicaid.gov/dq-atlas/downloads/background-and-methods/TAF-DQ-Proc-Cd-IP.pdf | **DQ Atlas B&M: Procedure Code — IP** | Background and methods for procedure code quality assessments on institutional and professional claims. |

**Note on Brief #5131:** The requested "DQ Brief #5131 on diagnosis codes" does not exist under that number. The correct resource is **Brief #5132** (below), plus the DQ Atlas Background and Methods documents for diagnosis codes listed above.

| URL | Title | Notes |
|-----|-------|-------|
| https://www.medicaid.gov/medicaid/data-and-systems/downloads/macbis/sud-databook-brief-5132.pdf | **Brief #5132: Missing and Invalid Diagnosis Codes in 2017** | SUD Data Book DQ brief evaluating diagnosis code completeness/validity across IP, LT, and OT claims for 2017 TAF data. Analyzes percentage of claims with valid ICD-10 primary diagnosis codes by state. |

### Additional DQ Atlas resources

| URL | Title | Notes |
|-----|-------|-------|
| https://www.medicaid.gov/about-us/messages/104931 | **DQ Atlas Introductory Blog Post** | CMS blog introducing DQ Atlas, its nine thematic areas, and how DQ Assessments, Background and Methods, and DQ Snapshots are organized. |
| https://www.medicaid.gov/medicaid/long-term-services-supports/downloads/ltss-users-taf-method-2023.pdf | **LTSS Data Quality Report (Mathematica, 2023)** | Analyzes LTSS-specific DQ measures for HCBS and institutional services in TAF, providing state-level ratings and recommendations. |
| https://resdac.org/sites/datadocumentation.resdac.org/files/2021-01/taf_dq_resource_transition.pdf | **DQ Resource Transition Guide** | Maps original 2016 TAF DQ briefs (released on ResDAC Nov 2019) to corresponding DQ Atlas topic areas. Essential for navigating between the old brief numbering and current DQ Atlas structure. |

---

## 3. ResDAC documentation

ResDAC (Research Data Assistance Center) maintains the most complete technical documentation for each TAF file type and serves as the gateway for researcher data access.

### TAF file pages (all verified working)

| URL | Title | Notes |
|-----|-------|-------|
| https://resdac.org/taf-data-quality-resources | **TAF Data Quality Resources Hub** | Central page linking to DQ Atlas, all TAF technical documentation, DQ briefs, and the DQ resource transition guide. |
| https://resdac.org/cms-data/files/taf-de | **TAF Demographic & Eligibility (DE)** | One record per individual eligible/enrolled for at least one day per year. Includes eligibility dates, managed care/waiver enrollment, disability info, and third-party liability. |
| https://resdac.org/cms-data/files/taf-ip | **TAF Inpatient (IP)** | Inpatient hospital stays including FFS claims, managed care encounters, and supplemental payments. Includes diagnosis codes, procedure codes, charges/payments, and discharge status. |
| https://resdac.org/cms-data/files/taf-lt | **TAF Long-Term Care (LT)** | Institutional long-term care from nursing facilities, mental facilities, psychiatric wings, and ICFs/IID. |
| https://resdac.org/cms-data/files/taf-ot | **TAF Other Services (OT)** | Physician services, outpatient hospital, lab/X-ray, clinic services, home health, hospice, and premium payments. The largest and most heterogeneous claims file. |
| https://resdac.org/cms-data/files/taf-rx | **TAF Pharmacy (RX)** | Filled prescriptions with NDC codes, days supply, charges/payments. Drug name, dosage, and format are NOT included — an external NDC source is needed. |
| https://resdac.org/cms-data/files/taf-apl | **TAF Annual Managed Care Plan (APL)** | Managed care organization characteristics including plan type, waivers, and eligible population indicators. |
| https://resdac.org/cms-data/files/taf-apr | **TAF Annual Provider (APR)** | Provider-level information including taxonomy indicators, residential treatment facility indicators, and SUD service provider indicators. |

### Technical documentation (with downloadable PDFs)

| URL | Title | Notes |
|-----|-------|-------|
| https://resdac.org/TAF-data-quality-resources/TAFTechDoc-CF | **Technical Documentation: Claims Files** | Development and content of IP, LT, OT, and RX claims files. |
| https://resdac.org/sites/datadocumentation.resdac.org/files/2022-06/TAF-TechGuide-Claims-Files.pdf | **Claims Files Tech Guide PDF (June 2022)** | Direct PDF download of claims file technical documentation. |
| https://resdac.org/TAF-data-quality-resources/TAFTechDoc-DEF | **Technical Documentation: DE File** | Development and content of the annual Demographic & Eligibility file. |
| https://resdac.org/sites/datadocumentation.resdac.org/files/2025-10/TAF-TechGuide-DE-File.pdf | **DE File Tech Guide PDF (Oct 2025)** | Most recent edition of DE file technical documentation. |
| https://resdac.org/TAF-data-quality-resources/TAFTechDoc-APL | **Technical Documentation: APL File** | Development and content of the Annual Managed Care Plan file. |
| https://resdac.org/sites/datadocumentation.resdac.org/files/2021-09/TAF_TechDoc_APL_File.pdf | **APL File Tech Guide PDF (Aug 2021)** | Direct PDF download for APL file documentation. |

### Data availability announcements

| URL | Title | Notes |
|-----|-------|-------|
| https://resdac.org/cms-news/2023-and-2024-t-msis-medicaid-and-chip-data-now-available | **2023 and 2024 T-MSIS Data Now Available (Dec 17, 2025)** | Announces fully mature 2023 TAF and preliminary 2024 TAF. Notes CMS now releases TAF on a regular annual December schedule. |
| https://resdac.org/cms-news/2022-and-2023-t-msis-medicaid-and-chip-data-now-available | **2022 and 2023 T-MSIS Data Now Available (Dec 19, 2024)** | Updated 2022 TAF (fully mature) and preliminary 2023 TAF. |
| https://resdac.org/cms-news/2021-and-2022-t-msis-medicaid-and-chip-data-now-available | **2021 and 2022 T-MSIS Data Now Available** | Updated 2021 TAF (fully mature) and preliminary 2022 TAF. |

### Additional ResDAC resources

| URL | Title | Notes |
|-----|-------|-------|
| https://resdac.org/workshops/intro-medicaid-chip-taf | **Workshop: Introduction to TAF Data** | Overview of TAF for research, covering Federal Medicaid benefits, state variation, available files, and data limitations. |
| https://www2.ccwdata.org/documents/10280/19002246/ccw-taf-rif-user-guide.pdf | **CCW TAF RIF User Guide (PDF)** | Chronic Conditions Data Warehouse user guide for TAF Research Identifiable Files. |

---

## 4. Research organizations

### KFF (Kaiser Family Foundation)

| URL | Title | Notes |
|-----|-------|-------|
| https://www.kff.org/medicaid/what-newly-released-medicaid-data-do-and-dont-tell-us/ | **What Newly Released Medicaid Data Do and Don't Tell Us (Feb 2026)** | Expert analysis of the DOGE dataset. Key finding: **10 of the 20 largest "providers" are state/local government agencies**, not healthcare providers. Data exclude hospital care (37% of Medicaid spending) and prescription drugs. |
| https://www.kff.org/medicaid/issue-brief/medicaid-administrative-data-challenges-with-race-ethnicity-and-other-demographic-variables/ | **Medicaid Administrative Data: Race/Ethnicity Challenges (Apr 2022)** | By Saunders and Chidambaram. Examines state variation in race/ethnicity data collection, voluntary self-reporting, and T-MSIS/TAF data quality issues. |

### SHADAC (State Health Access Data Assistance Center)

| URL | Title | Notes |
|-----|-------|-------|
| https://www.shadac.org/publications/medicaid-claims-data-t-msis-research-brief | **T-MSIS Info Brief Landing Page (Nov 2025)** | Explains what T-MSIS is, why it's useful for research, and cites studies on psychiatric ED visits, opioid use in pregnancy, and pediatric subspecialist access. |
| https://shadac-pdf-files.s3.us-east-2.amazonaws.com/s3fs-public/2025-11/T-MSIS%20Info%20Brief_FINAL.pdf | **T-MSIS Info Brief PDF (Nov 2025)** | Direct PDF download of the SHADAC info brief. |
| https://www.shadac.org/news/raceethnicity-data-cms-medicaid-t-msis-analytic-files-2020-data-assessment | **Race/Ethnicity 2020 Data Assessment** | 2020 TAF Release 1 analysis showing 4 states with "Unusable" ratings for race/ethnicity data. |
| https://www.shadac.org/news/race-ethnicity-data-tmsis-analytic-files-TAF-2022-data | **Race/Ethnicity 2022 Data Assessment** | Most recent assessment showing improvement: only 1 state "Unusable" (down from 4 in 2020), but 14 states at "High Concern." |
| https://www.shadac.org/news/race-ethnicity-data-tmsis-analytic-files-2021-data | **Race/Ethnicity 2021 Data Assessment** | Intermediate year assessment tracking improvement trends. |

### Commonwealth Fund

| URL | Title | Notes |
|-----|-------|-------|
| https://www.commonwealthfund.org/blog/2023/informing-medicaid-policy-better-claims-data | **Informing Medicaid Policy Through Better Claims Data (Jun 2023)** | By Gordon, McConnell, and Schpero. Outlines opportunities for improving Medicaid analytic data quality, accessibility, and usability based on MDLN findings. ⚠️ May return 403 for automated fetchers but loads in browsers. |

### GAO (Government Accountability Office)

| URL | Title | Notes |
|-----|-------|-------|
| https://www.gao.gov/products/gao-21-196 | **GAO-21-196 Product Page** | Landing page for GAO's T-MSIS data completeness report with summary and recommendations. |
| https://www.gao.gov/assets/gao-21-196.pdf | **GAO-21-196 Full Report PDF (Jan 2021)** | Found T-MSIS data completeness and accuracy have improved but gaps remain — **30 states didn't submit acceptable inpatient managed care encounter data**. Made 13 recommendations to CMS. |

### MACPAC (Medicaid and CHIP Payment and Access Commission)

| URL | Title | Notes |
|-----|-------|-------|
| https://www.macpac.gov/publication/update-on-transformed-medicaid-statistical-information-system-t-msis-2/ | **T-MSIS Update (Oct 2019) Landing Page** | Overview of T-MSIS and preliminary analyses assessing accuracy of Medicaid enrollment and spending using FY 2016 data. |
| https://www.macpac.gov/wp-content/uploads/2019/11/Update-on-Transformed-Medicaid-Statistical-Information-System-T-MSIS.pdf | **T-MSIS Update 2019 PDF** | Direct PDF of the October 2019 MACPAC presentation. |
| https://www.macpac.gov/publication/methodological-approaches-for-analyzing-use-and-spending-in-medicaid-long-term-services-and-supports-a-comparative-review-2/ | **LTSS Methodological Approaches (Aug 2024) Landing Page** | Comparative review of four analytic frameworks for LTSS data using TAF: CMS reports, ASPE taxonomy, DQ Atlas methodology, and KFF analyses. |
| https://www.macpac.gov/wp-content/uploads/2024/08/Methodological-Approaches-for-Analyzing-Use-and-Spending-in-Medicaid-Long-Term-Services-and-Supports-A-Comparative-Review.pdf | **LTSS Methodological Approaches PDF** | Direct PDF download of the comparative review. |
| https://www.macpac.gov/macstats/data-sources-and-methods/ | **MACStats Technical Guide Landing Page** | Supplementary information on data sources and methods used in MACStats exhibits, including T-MSIS analysis methodology. |
| https://www.macpac.gov/wp-content/uploads/2022/12/MACStats-2022-Technical-Guide.pdf | **MACStats 2022 Technical Guide PDF** | December 2022 edition with T-MSIS eligibility group assignments and benefit spending adjustment methodology. |
| https://www.macpac.gov/wp-content/uploads/2023/12/Technical-Guide-to-MACStats-December-2023.pdf | **MACStats 2023 Technical Guide PDF** | December 2023 edition. |
| https://www.macpac.gov/wp-content/uploads/2026/02/MACSTATS_Feb2026_WEB_508.pdf | **MACStats February 2026 Edition PDF** | Most recent MACStats data book. |
| https://www.macpac.gov/macstats/ | **MACStats Main Portal** | Central portal for all MACStats data book downloads. |

### RTI International

| URL | Title | Notes |
|-----|-------|-------|
| https://www.rti.org/insights/medicaid-data-quality-analysis-cms-taf | **Quick Guide to Checking TAF Data Quality** | By Beil and Spencer. Practical process for assessing TAF data quality for analysis — evaluating completeness, validity, consistency, reliability, and accuracy with worked examples. |

### ASHEcon (American Society of Health Economists)

| URL | Title | Notes |
|-----|-------|-------|
| https://www.ashecon.org/newsletter/catalyzing-the-future-of-medicaid-research-the-t-msis-analytic-files/ | **Catalyzing the Future of Medicaid Research: The T-MSIS Analytic Files** | By William Schpero (Weill Cornell). Comprehensive overview tracing evolution from MSIS to T-MSIS, explaining data structure, availability, and TAF's potential for Medicaid policy research. |

### AcademyHealth and MDLN (Medicaid Data Learning Network)

| URL | Title | Notes |
|-----|-------|-------|
| https://academyhealth.org/publications/2023-06/catalyzing-medicaid-policy-research-t-msis-analytic-files-taf-learnings-year-1-medicaid-data-learning-network-mdln | **MDLN Year 1 Summary Report (Jun 2023)** | By Gordon, Johnson, Kennedy, McConnell, and Schpero. Summary of eight virtual learning sessions covering DQ best practices, eligibility definitions, and consensus methods for TAF research. |
| https://academyhealth.org/publications/2025-10/t-msis-analytic-files-taf-analysis-reporting-checklist | **TAF Analysis Reporting Checklist (Oct 2025)** | Downloadable 4-category checklist: (1) data details, (2) analytic sample definition, (3) state exclusions based on DQ, and (4) special considerations for spending data and DQ changes over time. |
| https://academyhealth.org/publications/2025-10/academyhealths-medicaid-data-learning-network-develops-new-t-msis-analytic-files-taf-analysis-reporting-checklist | **TAF Checklist Announcement Page** | Companion announcement describing the checklist's development and purpose. |
| https://academyhealth.org/publications/2024-01/open-source-code-t-msis-analytic-files-taf | **Open Source Code for TAF (Jan 2024)** | MDLN report on the GitHub repository of open-source R functions (OHSU taf.functions), including DQ Atlas replication tools and ICD-10 lookups for opioid use disorder. |
| https://academyhealth.org/about/programs/medicaid-data-learning-network | **MDLN Program Page** | Overview of the Medicaid Data Learning Network and its mission. |

### PubMed / JAMA Health Forum

| URL | Title | Notes |
|-----|-------|-------|
| https://pubmed.ncbi.nlm.nih.gov/41134558/ | **PubMed: TAF Analysis Reporting Checklist** | Schpero WL, McConnell KJ, Bushnell G, et al. *JAMA Health Forum*. 2025;6(10):e253622. The first standardized reporting guideline for TAF-based Medicaid research. |
| https://jamanetwork.com/journals/jama-health-forum/fullarticle/2840338 | **JAMA Health Forum Full Article** | Full text of the TAF Analysis Reporting Checklist Special Communication. |
| https://pmc.ncbi.nlm.nih.gov/articles/PMC8378645/ | **PMC: Data Characterization of Medicaid — Legacy and New Formats** | Peer-reviewed comparison of MSIS/MAX vs. T-MSIS/TAF formats, with practical guidance for researchers using the CMS VRDC. |

### HHS/ASPE

| URL | Title | Notes |
|-----|-------|-------|
| https://aspe.hhs.gov/sites/default/files/documents/5ddae662bdcd7b04379e6f176a283441/identify-classify-hcbs-claims-tmsis-brief.pdf | **ASPE: Identifying and Classifying HCBS Claims in T-MSIS (Sep 2023)** | Taxonomy for classifying HCBS claims using procedure codes, modifiers, place of service, and type of service codes. Builds on the MAX HCBS taxonomy. |

### Urban Institute

| URL | Title | Notes |
|-----|-------|-------|
| https://www.urban.org/research/publication/dual-medicare-medicaid-enrollment-and-integrated-plan-identification | **Dual Medicare-Medicaid Enrollment in TAF** | Investigates 2016 TAF dual enrollment data quality by comparing to Medicare MBSF. Found ~84% agreement but significant state-level variation. ⚠️ May return 403 on automated fetch. |

---

## 5. GitHub repositories

All five repositories were **verified as public and accessible**.

| URL | Title | Notes |
|-----|-------|-------|
| https://github.com/CMSgov/T-MSIS-Analytic-File-Generation-Code | **TAF Generation Code (SAS/Redshift SQL)** | Official CMS repository with SAS code and Redshift SQL passthrough that generates the TAF. Provides transparency into TAF generation logic. Requires CMS AWS Redshift environment. License: CC0-1.0. |
| https://github.com/Enterprise-CMCS/T-MSIS-Analytic-File-Generation-Python | **TAF Generation Code (Python/Databricks)** | Python library for generating TAF using Databricks, replacing/complementing the SAS code. Files can run independently in Notebooks, grouped by state or time interval. **5 stars, 4 forks.** License: CC0-1.0. |
| https://github.com/CMSgov/T-MSIS-Data-Quality-Measures-Generation-Code | **DQ Measures Generation Code (Python/Databricks SQL)** | Code for calculating T-MSIS Data Quality measures including threshold definitions (Thresholds.xlsx), measure specifications, and lookup tables. Runs on DataConnect (CMCS data warehouse). License: CC0-1.0. |
| https://github.com/chse-ohsu/taf.functions | **taf.functions R Package (OHSU)** | R package from Oregon Health & Science University providing reusable functions for TAF data preparation and analysis, including DQ measure functions and opioid use disorder identification using NQF-12 ICD codes. **6 stars.** License: MIT. Install: `devtools::install_github('chse-ohsu/taf.functions', subdir = 'pkg')` |
| https://github.com/astoreyai/medicaid-kg | **medicaid-kg: Interactive Knowledge Graph** | Full-stack app combining NetworkX knowledge graph (~550K nodes, ~4.7M edges) with DuckDB-powered queries over **227M rows ($1.09T)** of HHS Medicaid Provider Spending data. Features React Flow, Deck.gl, Claude AI agent personas with MCP tools, and time-slider animation. |

---

## 6. February 2026 HHS/DOGE Medicaid Provider Spending dataset

On **February 13, 2026**, the DOGE team within HHS released provider-level Medicaid spending data aggregated from T-MSIS outpatient and professional claims with valid HCPCS codes, covering January 2018 through December 2024. The dataset contains **over 227 million rows (~10 GB)** with fields for billing/servicing provider NPI, HCPCS code, year-month, unique beneficiaries, total claims, and total paid amount. As of early March 2026, the HHS portal shows the dataset as **"temporarily unavailable while we make improvements."**

### Primary data access

| URL | Title | Notes |
|-----|-------|-------|
| https://opendata.hhs.gov/datasets/medicaid-provider-spending/ | **HHS Open Data Portal: Medicaid Provider Spending** | Official release page. ⚠️ **Temporarily offline** as of ~March 3, 2026 ("temporarily unavailable while we make improvements"). |
| https://huggingface.co/datasets/HHS-Official/medicaid-provider-spending | **HHS-Official Hugging Face Mirror** | Official HHS account upload. 227M rows, CC0-1.0 license. Raw dataset with original column names. **Working.** |
| https://huggingface.co/datasets/cfahlgren1/medicaid-provider-spending | **cfahlgren1 Enriched Hugging Face Mirror** | 229M rows across 4 splits: spending (227M), billing_providers (618k), servicing_providers (1.63M), and hcpcs_codes (7.55k). Includes enriched data with provider name lookups. SQL console and Data Studio viewer available. **Working.** |
| https://getmedicaiddata.com/ | **Medicaid Data Services (Third-Party)** | By Christy Warner (former CMS Fraud Prevention System developer). Parsed dataset by state and joined with NPPES NPI Registry to add provider names, addresses, and phone numbers. |

### News coverage

| URL | Title | Notes |
|-----|-------|-------|
| https://www.axios.com/2026/02/14/elon-musk-doge-medicaid-fraud-hhs-database | **Axios: "Elon Musk declares victory with Medicaid data release" (Feb 14, 2026)** | By Bettelheim and Goldman. Covers Musk's X post, HHS spokesman Andrew Nixon's statement, and DOGE's year-long access to HHS data systems. ⚠️ May return 403 (paywall). |
| https://distilinfo.com/2026/02/16/musks-doge-releases-historic-medicaid-dataset/ | **DistilInfo: "Musk's DOGE Releases Historic Medicaid Dataset" (Feb 16, 2026)** | Covers the 10.32 GB release, fraud detection crowdsourcing angle, and Treasury Secretary Bessent's whistleblower reward plans (10–30% of recovered fines). |
| https://www.benzinga.com/news/health-care/26/02/50631736/doge-open-sources-largest-medicaid-dataset-in-agency-history-as-elon-musk-touts-transparency-move-its-a-state-of-mind | **Benzinga: DOGE Open-Sources Largest Medicaid Dataset (Feb 13, 2026)** | Published same day as release. Covers dataset structure, cell suppression threshold (rows with fewer than 12 claims dropped), and T-MSIS DQ caveats. |

### Expert analysis and commentary

| URL | Title | Notes |
|-----|-------|-------|
| https://www.mcdermottplus.com/blog/regs-eggs/over-easy-or-under-done-first-look-at-doges-medicaid-data/ | **McDermott+: "Over easy or undercooked?" (Feb 26, 2026)** | By Davis and Livshen. **The most thorough independent analysis.** Key findings: 2024 data likely incomplete (sharp Nov-Dec dropoff); managed care encounter data may be incomplete; excludes inpatient, LTC, pharmacy, and dental; "payment per claim" metric is misleading due to billing practice differences. |
| https://www.jdsupra.com/legalnews/over-easy-or-undercooked-first-look-at-7847545/ | **McDermott+ on JD Supra (republication)** | Same analysis republished on JD Supra legal platform. |
| https://www.hfma.org/legal-and-regulatory-compliance/medicaid-billing-data-transparency-cms/ | **HFMA: "CMS releases Medicaid billing data to expand fraud oversight"** | Covers 227M+ rows, 1.8M NPIs. Includes DOGE administrator Amy Gleason's comments. Confirms data went offline ~March 3, 2026. Also covers the $259.5M Minnesota Medicaid deferral. |
| https://www.morganlewis.com/pubs/2026/02/cms-announces-sweeping-anti-healthcare-fraud-initiatives | **Morgan Lewis: CMS Anti-Fraud Initiatives (Feb 2026)** | Legal perspective on enforcement actions. Raises concerns about providers' ability to appeal revocations given reduced CMS staffing. |
| https://www.foxnews.com/politics/doges-medicaid-data-dump-aims-expose-fraud-privacy-legal-hurdles-loom | **Fox News: Legal Hurdles After DOGE Data Release** | Focuses on patient privacy, proof standards, and uneven state data quality. Notes DOJ healthcare fraud strike force now in 25 federal districts. |

### Deep-dive analyses

| URL | Title | Notes |
|-----|-------|-------|
| https://www.onhealthcare.tech/p/the-800b-open-secret-what-the-new | **On Healthcare Tech: "The $800B Open Secret"** | Technical and investment analysis. Notes $849B total Medicaid spend, $31.1B improper payments in FY2024. Warns of "political instability of the data asset" as a risk for builders. |
| https://www.investigativeeconomics.org/p/egregious-medicaid-spending-anomalies | **Investigative Economics: Spending Anomalies** | Examples of outlier spending (e.g., $4.7M in syringes for 22 recipients). Notes that flagged providers were already known to HHS and some had been previously prosecuted. |
| https://dutchrojas.substack.com/p/the-billion-dollar-club | **The Rojas Report: "The Billion-Dollar Club"** | Detailed analysis of HCPCS code T1019 (Personal Care Services). Found **9 entities crossing the billion-dollar mark, 7 in Brooklyn** — geographic concentration raising fraud concerns. |

### Podcasts and multimedia

| URL | Title | Notes |
|-----|-------|-------|
| https://www.mcdermottplus.com/podcasts/health-policy-breakroom/deep-dive-medicaid-data-dump/ | **McDermott+ Podcast: "Deep dive: Medicaid data dump"** | Audio companion to the written "Over easy or undercooked?" analysis. |
| https://www.mcdermottplus.com/podcasts/healthcare-preview-podcast/congressional-recess-doges-medicaid-data-dump/ | **McDermott+ Podcast: Congressional Recess and DOGE** | Healthcare Preview podcast episode on the political context. |

### Social media (original announcements)

| URL | Title | Notes |
|-----|-------|-------|
| https://x.com/DOGE_HHS/status/2022370909211021376 | **DOGE HHS X Post** | Original announcement post from the DOGE HHS account. |
| https://x.com/elonmusk/status/2022416233644367898 | **Elon Musk X Post** | Musk's post amplifying the dataset release. |

---

## 7. Mathematica reports (additional high-quality resources)

Mathematica is CMS's primary contractor for TAF generation and data quality work. These resources provide critical context for T-MSIS data management.

| URL | Title | Notes |
|-----|-------|-------|
| https://www.mathematica.org/en/publications/using-t-msis-and-taf-to-elevate-medicaid-program-integrity | **Using T-MSIS and TAF to Elevate Medicaid Program Integrity (Jul 2025)** | White paper on how state program integrity units can use TAF to detect fraud, waste, and abuse at national scale. |
| https://www.mathematica.org/en/publications/report-to-congress-t-msis-substance-use-disorder-sud-data-book-treatment-of-sud-in-medicaid-2021 | **SUD Data Book 2021 (Report to Congress)** | Fifth annual SUPPORT Act report: **4.9M Medicaid beneficiaries treated for SUD**, including 1.8M for opioid use disorder. |
| https://www.mathematica.org/publications/report-to-congress-t-msis-substance-use-disorder-sud-data-book-treatment-of-sud-in-medicaid-2020 | **SUD Data Book 2020 (Report to Congress)** | Fourth annual report: 4.6M Medicaid beneficiaries treated for SUD in 2020. |
| https://www.mathematica.org/publications/taf-technical-guidance-how-to-use-illinois-claims-data | **TAF Technical Guidance: Illinois Claims Data** | State-specific guidance on Illinois's novel method for reporting claim adjustments to T-MSIS and its impact on TAF users. |
| https://www.mathematica.org/blogs/advances-in-medicaid-data-provide-timely-insights-into-the-pandemic | **Advances in Medicaid Data During COVID** | How TAF infrastructure enabled CMS to monitor real-time pandemic impacts on Medicaid/CHIP enrollment and service use. |

---

## Key observations for T-MSIS practitioners

Several cross-cutting themes emerge from this resource compilation that are essential for anyone working with T-MSIS data. **The OBA framework has formally replaced the 32 T-MSIS Priority Items** as of SHO #25-002 (May 2025), which means older references to TPIs should be understood as superseded. The DQ Atlas remains the authoritative source for state-level data quality assessments, now covering 80+ topics across nine thematic areas.

For the DOGE dataset specifically, the McDermott+ and KFF analyses are the most authoritative independent evaluations. Both identify critical limitations: the data covers only OT (Other Services) file claims with valid HCPCS codes, **excluding inpatient stays, long-term care, pharmacy claims, and dental services** — which together represent the majority of Medicaid spending. The 2024 data shows a sharp dropoff in November-December, suggesting incomplete run-out, and managed care encounter completeness varies dramatically by state.

The **TAF Analysis Reporting Checklist** (Schpero et al., JAMA Health Forum 2025) represents a landmark consensus document that should be the starting point for any research project using TAF data, providing the first standardized methodology for documenting data quality decisions in TAF-based analyses.