// ── Plain-Language Synonym Map ──────────────────────────────────────────
// Maps everyday service terms → HCPCS codes, categories, or description fragments.
// Extracted from TmsisExplorer.tsx for reuse across search, Intelligence, and modules.

export const SYNONYMS: Record<string, string[]> = {
  // Home & community-based
  "home care": ["T1019","T2025","S5130","S5125","T1020","HCBS"],
  "home health": ["T1019","T2025","S5130","S5125","T1020","HCBS"],
  "personal care": ["T1019","T1020","S5130","S5125","HCBS"],
  "waiver": ["T1019","T2025","T2026","T2027","HCBS","Waiver"],
  "hcbs": ["T1019","T2025","T2026","S5130","HCBS","Waiver"],
  "respite": ["T1005","S5150","S5151","HCBS"],
  "day program": ["T2021","S5100","S5102","HCBS"],

  // Behavioral health
  "autism": ["97153","97151","97152","97154","97155","97156","ABA"],
  "aba": ["97153","97151","97152","97154","97155","97156"],
  "behavioral health": ["90834","90837","90832","97153","H0031","H0032","Behavioral"],
  "mental health": ["90834","90837","90832","90847","H0031","H0032","Behavioral"],
  "therapy": ["90834","90837","97110","97140","97530","97153"],
  "counseling": ["90834","90837","90832","90847","H0004"],
  "psychotherapy": ["90834","90837","90832","90846","90847"],

  // Dental
  "dental": ["D0120","D0150","D0210","D0220","D1110","D2391","D7140","Dental"],
  "teeth": ["D0120","D1110","D2391","D7140","Dental"],
  "cleaning": ["D1110","D0120","Dental"],
  "filling": ["D2391","D2392","D2140","Dental"],
  "extraction": ["D7140","D7210","Dental"],

  // E&M / office
  "office visit": ["99213","99214","99215","99211","99212","E&M"],
  "doctor visit": ["99213","99214","99215","E&M"],
  "checkup": ["99213","99395","99393","99214","E&M"],
  "well child": ["99393","99392","99391","99395"],

  // Maternity
  "pregnancy": ["59400","59510","59025","59430","Maternity"],
  "maternity": ["59400","59510","59025","59430","Maternity"],
  "prenatal": ["59400","59425","59025","Maternity"],
  "birth": ["59400","59510","Maternity"],
  "delivery": ["59400","59510","Maternity"],
  "c-section": ["59510","59515","Maternity"],

  // Drugs & pharmacy
  "drugs": ["J3490","J0129","J1745","J2505","Drugs"],
  "medication": ["J3490","J0129","J1745","Drugs"],
  "injection": ["J3490","J0129","96372","Drugs"],
  "vaccine": ["90460","90461","90471","90472","Immunization"],
  "immunization": ["90460","90461","90471","Immunization"],

  // Imaging
  "imaging": ["70553","74177","72148","Imaging"],
  "x-ray": ["71046","73030","70100","Imaging"],
  "mri": ["70553","72148","73721","Imaging"],
  "ct scan": ["74177","70551","72131","Imaging"],

  // Lab
  "lab": ["80053","85025","80048","Lab"],
  "blood test": ["85025","80053","80048","Lab"],

  // Rehab
  "physical therapy": ["97110","97140","97530","97112","Rehab"],
  "pt": ["97110","97140","97530","Rehab"],
  "occupational therapy": ["97530","97535","97542","Rehab"],
  "speech therapy": ["92507","92508","92521","92522"],
  "speech": ["92507","92508","92521","92522"],

  // DME & transport
  "wheelchair": ["K0001","K0002","K0003","K0004","DME"],
  "dme": ["K0001","E0601","E0260","DME"],
  "ambulance": ["A0427","A0429","A0433","Transport"],
  "transport": ["A0427","A0429","T2003","Transport"],

  // Facility & acute
  "nursing": ["T1030","T1031","99211"],
  "dialysis": ["90935","90937","90945"],
  "emergency": ["99281","99282","99283","99284","99285"],
  "er": ["99281","99282","99283","99284","99285"],
  "hospital": ["99221","99222","99223","99231","99232"],
  "inpatient": ["99221","99222","99223","99231"],
  "surgery": ["Surgery","27447","43239","47562"],

  // Vision & hearing
  "vision": ["92014","92004","S0580","Vision"],
  "eye exam": ["92014","92004","Vision"],
  "glasses": ["S0580","V2020","V2100","Vision"],
  "hearing": ["92557","V5008","V5261"],

  // Respiratory
  "respiratory": ["94010","94060","E0601"],
  "asthma": ["94010","94060","94640"],

  // Chronic / specialty
  "diabetes": ["99214","80053","83036","82947"],
  "pain management": ["20610","64483","97140"],
  "telehealth": ["99213","99214","GT","95"],
};

// ── Reverse map: HCPCS code → service names ────────────────────────────
// Useful for labeling codes with human-readable service names.
export const HCPCS_TO_SERVICE: Record<string, string[]> = {};

for (const [service, codes] of Object.entries(SYNONYMS)) {
  for (const code of codes) {
    // Only map actual HCPCS-like codes (skip category labels like "Dental", "E&M")
    if (/^[A-Z0-9]{2,5}$/i.test(code) || /^[A-Z]\d{4}$/i.test(code)) {
      if (!HCPCS_TO_SERVICE[code]) HCPCS_TO_SERVICE[code] = [];
      if (!HCPCS_TO_SERVICE[code].includes(service)) {
        HCPCS_TO_SERVICE[code].push(service);
      }
    }
  }
}
