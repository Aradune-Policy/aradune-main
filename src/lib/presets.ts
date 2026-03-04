/**
 * Preset filter definitions for common query patterns.
 * Ported from server/presets.py for browser-side use.
 */

import type { PresetInfo } from "../types";

// ── CCBHC code lists from Milliman/AHCA Appendix I (April 2025) ──────
const CCBHC_CORE = [
  // Screening, Assessment, and Diagnosis
  "H2000", "H0031", "H0001",
  // Person-Centered and Family-Centered Treatment Planning
  "H0032", "T1007",
  // Outpatient Mental Health and Substance Use Services
  "H0015", "H0020", "H2019", "H0040", "H2033",
  // Primary Care Screening and Monitoring
  "99385", "99386", "99387", "99395", "99396", "99397", "96110",
  // Targeted Case Management
  "T1017",
  // Psychiatric Rehabilitation Services
  "H2017",
  // Peer Supports and Family/Caregiver Supports
  "H0018", "H0038",
  // Crisis Services
  "H2011", "S9484",
];

const CCBHC_EXPANDED_ONLY = [
  // Screening, Assessment, and Diagnosis (expanded)
  "H2010", "96160", "96130", "96131", "96132", "96133",
  // Outpatient MH and SU Services (expanded)
  "H0046", "H0047", "H0048", "H2012", "T1015", "T1023", "S9480",
  // Psychiatric Rehabilitation (expanded)
  "H2030",
  // Peer Supports (expanded)
  "S5102",
  // Other Medical Items or Services
  "H0035", "S9475",
];

const CCBHC_ALL = [...new Set([...CCBHC_CORE, ...CCBHC_EXPANDED_ONLY])];

export const PRESETS: Record<string, PresetInfo> = {
  all_services: {
    id: "all_services",
    name: "All Services",
    description: "Total Medicaid spending across all service categories. Aggregate view by state, year, or month",
    codes: [],
    filter_type: "ordering",
  },
  ccbhc_core: {
    id: "ccbhc_core",
    name: "CCBHC Core",
    description: "Core CCBHC services per Milliman/AHCA Appendix I: the 9 required SAMHSA service categories",
    codes: CCBHC_CORE,
    filter_type: "hcpcs_codes",
  },
  ccbhc_expanded: {
    id: "ccbhc_expanded",
    name: "CCBHC Expanded",
    description: "Core + Expanded CCBHC services per Milliman/AHCA Appendix I. Includes additional EBPs and service types",
    codes: CCBHC_ALL,
    filter_type: "hcpcs_codes",
  },
  hcbs_waiver: {
    id: "hcbs_waiver",
    name: "HCBS / Waiver",
    description: "Home and community-based services and waiver program codes",
    codes: [
      "T1019", "T1020", "T2025", "T2026", "T2027", "T2028",
      "S5130", "S5125", "S5150", "S5151",
      "T1005", "T2021", "S5100", "S5102",
      "T2003", "T1030", "T1031",
    ],
    filter_type: "hcpcs_codes",
  },
  behavioral_health: {
    id: "behavioral_health",
    name: "Behavioral Health",
    description: "Mental health, substance use, and ABA therapy codes",
    codes: [
      "90832", "90834", "90837", "90846", "90847", "90853",
      "90839", "90840", "90791", "90792",
      "97151", "97152", "97153", "97154", "97155", "97156", "97158",
      "H0004", "H0005", "H0015", "H0020", "H0031", "H0032",
      "H0038", "H2017", "H2018",
    ],
    filter_type: "hcpcs_codes",
  },
  dental: {
    id: "dental",
    name: "Dental",
    description: "Common dental procedure codes",
    codes: [
      "D0120", "D0150", "D0210", "D0220", "D0230", "D0272", "D0274",
      "D1110", "D1120", "D1208",
      "D2140", "D2150", "D2391", "D2392",
      "D3220", "D3310",
      "D7140", "D7210",
    ],
    filter_type: "hcpcs_codes",
  },
  em: {
    id: "em",
    name: "E&M Services",
    description: "Evaluation and management office visit codes",
    codes: [
      "99202", "99203", "99204", "99205",
      "99211", "99212", "99213", "99214", "99215",
      "99281", "99282", "99283", "99284", "99285",
      "99381", "99382", "99383", "99384", "99385",
      "99391", "99392", "99393", "99394", "99395",
    ],
    filter_type: "hcpcs_codes",
  },
  top_spending: {
    id: "top_spending",
    name: "Top Spending",
    description: "Query sorted by total spending (no code filter, just ordering)",
    codes: [],
    filter_type: "ordering",
  },
  rehabilitation: {
    id: "rehabilitation",
    name: "Rehabilitation",
    description: "PT/OT/Speech therapy access: physical, occupational, and speech-language pathology codes",
    codes: [
      "97110", "97112", "97116", "97140", "97530", "97535",
      "97161", "97162", "97163", "97164",
      "92507", "92508", "92521", "92522", "92523", "92524", "92525", "92526",
    ],
    filter_type: "hcpcs_codes",
  },
  telehealth: {
    id: "telehealth",
    name: "Telehealth",
    description: "Post-COVID telehealth expansion: audio/video visit codes, e-visits, and common telehealth-delivered services",
    codes: [
      "99441", "99442", "99443",
      "98966", "98967", "98968",
      "99421", "99422", "99423",
      "G2012", "G2010",
      "90834", "90837", "99213", "99214",
    ],
    filter_type: "hcpcs_codes",
  },
  mat_opioid: {
    id: "mat_opioid",
    name: "MAT / Opioid",
    description: "Opioid epidemic treatment: medication-assisted treatment, buprenorphine, naltrexone, and OTP codes",
    codes: [
      "H0020", "H0033",
      "J0571", "J0572", "J0573", "J0574", "J0575",
      "J2315",
      "G2067", "G2068", "G2069", "G2070", "G2071", "G2072", "G2073", "G2074", "G2075", "G2076", "G2077", "G2078", "G2079", "G2080",
      "99205", "99215",
    ],
    filter_type: "hcpcs_codes",
  },
  maternity: {
    id: "maternity",
    name: "Maternity / OB",
    description: "Prenatal, delivery, and postpartum care: global OB packages, ultrasound, and cesarean codes",
    codes: [
      "59400", "59410", "59425", "59426",
      "59510", "59515", "59025", "59430",
      "59610", "59612", "59614", "59618", "59620", "59622",
      "76801", "76802", "76805", "76810", "76811", "76812", "76813", "76814",
    ],
    filter_type: "hcpcs_codes",
  },
  lab_imaging: {
    id: "lab_imaging",
    name: "Lab / Imaging",
    description: "Diagnostic access disparities: common lab panels, urinalysis, radiology, and cardiac diagnostics",
    codes: [
      "80053", "80048", "85025", "83036", "80061",
      "81001", "81002",
      "71046", "70553", "72148", "74177", "76700",
      "93000",
    ],
    filter_type: "hcpcs_codes",
  },
  transportation: {
    id: "transportation",
    name: "NEMT / Transport",
    description: "Non-emergency medical transportation access: ambulance, stretcher van, and NEMT broker codes",
    codes: [
      "A0427", "A0429", "A0433", "A0425", "A0426", "A0428",
      "T2003", "T2005",
      "A0080", "A0090", "A0100", "A0110", "A0120", "A0130",
      "S0209",
    ],
    filter_type: "hcpcs_codes",
  },
  dme: {
    id: "dme",
    name: "DME",
    description: "Durable medical equipment access: wheelchairs, CPAP, hospital beds, walkers, diabetic supplies, and orthotics",
    codes: [
      "K0001", "K0002", "K0003", "K0004",
      "E0601", "E0260", "E0100", "E0105", "E0143", "E0148",
      "A4253", "A4259",
      "L3000", "L3010", "L3020",
    ],
    filter_type: "hcpcs_codes",
  },
};

export function getPreset(id: string): PresetInfo | undefined {
  return PRESETS[id];
}

export function listPresets(): PresetInfo[] {
  return Object.values(PRESETS);
}
