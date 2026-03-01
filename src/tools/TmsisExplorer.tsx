import React, { useState, useMemo, useEffect, useCallback, useRef, Fragment } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, CartesianGrid, Area, AreaChart, LineChart, Line, ScatterChart, Scatter, ZAxis, Legend, ReferenceLine } from "recharts";
import type { StateData, HcpcsCode, NatlTrend, SafeTipProps, CatAccumulator, TooltipEntry, RawState, RawHcpcs, RawTrend, PipelineMeta, MedicareRates, RiskAdjData, FeeScheduleData, FeeScheduleState, FeeScheduleDirectory, ProviderRecord, SpecialtyRecord } from "../types";

// ── Design System (Aradune v13) ──────────────────────────────────────────
const A = "#0A2540";
const AL = "#425A70";
const POS = "#2E6B4A";
const NEG = "#A4262C";
const WARN = "#B8860B";
const S = "#F5F7F5";
const B = "#E4EAE4";
const WH = "#fff";
const cB = "#2E6B4A";
const cG = "#1B5E3A";
const cO = "#C4590A";
const cT = "#3A7D5C";
const FM = "'SF Mono',Menlo,monospace";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

// ── Plain-Language Synonym Map ──────────────────────────────────────────
// Maps everyday terms → HCPCS codes, categories, or description fragments
const SYNONYMS = {
  // Service areas
  "home care":["T1019","T2025","S5130","S5125","T1020","HCBS"],
  "home health":["T1019","T2025","S5130","S5125","T1020","HCBS"],
  "personal care":["T1019","T1020","S5130","S5125","HCBS"],
  "waiver":["T1019","T2025","T2026","T2027","HCBS","Waiver"],
  "hcbs":["T1019","T2025","T2026","S5130","HCBS","Waiver"],
  "autism":["97153","97151","97152","97154","97155","97156","ABA"],
  "aba":["97153","97151","97152","97154","97155","97156"],
  "behavioral health":["90834","90837","90832","97153","H0031","H0032","Behavioral"],
  "mental health":["90834","90837","90832","90847","H0031","H0032","Behavioral"],
  "therapy":["90834","90837","97110","97140","97530","97153"],
  "counseling":["90834","90837","90832","90847","H0004"],
  "psychotherapy":["90834","90837","90832","90846","90847"],
  "dental":["D0120","D0150","D0210","D0220","D1110","D2391","D7140","Dental"],
  "teeth":["D0120","D1110","D2391","D7140","Dental"],
  "cleaning":["D1110","D0120","Dental"],
  "filling":["D2391","D2392","D2140","Dental"],
  "extraction":["D7140","D7210","Dental"],
  "office visit":["99213","99214","99215","99211","99212","E&M"],
  "doctor visit":["99213","99214","99215","E&M"],
  "checkup":["99213","99395","99393","99214","E&M"],
  "well child":["99393","99392","99391","99395"],
  "pregnancy":["59400","59510","59025","59430","Maternity"],
  "maternity":["59400","59510","59025","59430","Maternity"],
  "prenatal":["59400","59425","59025","Maternity"],
  "birth":["59400","59510","Maternity"],
  "delivery":["59400","59510","Maternity"],
  "c-section":["59510","59515","Maternity"],
  "drugs":["J3490","J0129","J1745","J2505","Drugs"],
  "medication":["J3490","J0129","J1745","Drugs"],
  "injection":["J3490","J0129","96372","Drugs"],
  "vaccine":["90460","90461","90471","90472","Immunization"],
  "immunization":["90460","90461","90471","Immunization"],
  "imaging":["70553","74177","72148","Imaging"],
  "x-ray":["71046","73030","70100","Imaging"],
  "mri":["70553","72148","73721","Imaging"],
  "ct scan":["74177","70551","72131","Imaging"],
  "lab":["80053","85025","80048","Lab"],
  "blood test":["85025","80053","80048","Lab"],
  "physical therapy":["97110","97140","97530","97112","Rehab"],
  "pt":["97110","97140","97530","Rehab"],
  "occupational therapy":["97530","97535","97542","Rehab"],
  "speech therapy":["92507","92508","92521","92522"],
  "speech":["92507","92508","92521","92522"],
  "wheelchair":["K0001","K0002","K0003","K0004","DME"],
  "dme":["K0001","E0601","E0260","DME"],
  "ambulance":["A0427","A0429","A0433","Transport"],
  "transport":["A0427","A0429","T2003","Transport"],
  "nursing":["T1030","T1031","99211"],
  "dialysis":["90935","90937","90945"],
  "emergency":["99281","99282","99283","99284","99285"],
  "er":["99281","99282","99283","99284","99285"],
  "hospital":["99221","99222","99223","99231","99232"],
  "inpatient":["99221","99222","99223","99231"],
  "surgery":["Surgery","27447","43239","47562"],
  "vision":["92014","92004","S0580","Vision"],
  "eye exam":["92014","92004","Vision"],
  "glasses":["S0580","V2020","V2100","Vision"],
  "hearing":["92557","V5008","V5261"],
  "respiratory":["94010","94060","E0601"],
  "asthma":["94010","94060","94640"],
  "diabetes":["99214","80053","83036","82947"],
  "pain management":["20610","64483","97140"],
  "telehealth":["99213","99214","GT","95"],
  "respite":["T1005","S5150","S5151","HCBS"],
  "day program":["T2021","S5100","S5102","HCBS"],
};

function expandSearch(query: string, codes: HcpcsCode[]): HcpcsCode[] {
  if (!query) return codes;
  const lq = query.toLowerCase().trim();
  // Direct code/description/category match
  const direct = codes.filter(h =>
    h.c.toLowerCase().includes(lq) ||
    h.d.toLowerCase().includes(lq) ||
    h.cat.toLowerCase().includes(lq)
  );
  // Synonym expansion (require 2+ chars to avoid matching everything)
  const synMatches = new Set<string>();
  if (lq.length >= 2) {
    for (const [term, targets] of Object.entries(SYNONYMS)) {
      if (term.includes(lq) || lq.includes(term)) {
        for (const t of targets) synMatches.add(t.toUpperCase());
      }
    }
  }
  if (synMatches.size === 0) return direct;
  const synCodes = codes.filter(h =>
    synMatches.has(h.c.toUpperCase()) ||
    synMatches.has(h.cat.toUpperCase()) ||
    [...synMatches].some(s => h.d.toUpperCase().includes(s as string))
  );
  // Merge: direct first, then synonym hits not already included
  const seen = new Set(direct.map(h => h.c));
  return [...direct, ...synCodes.filter(h => !seen.has(h.c))];
}

const f$ = (v: number): string => {
  if (v == null || isNaN(v) || !isFinite(v)) return "$0";
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(1)}T`;
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${abs.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  if (abs < 10) return `${sign}$${abs.toFixed(2)}`;
  return `${sign}$${abs.toFixed(0)}`;
};
// Super-compact formatter for hex map (max ~6 chars)
const f$c = (v: number): string => {
  if (v == null || isNaN(v) || !isFinite(v)) return "$0";
  if (v >= 1e12) return `$${(v / 1e12).toFixed(0)}T`;
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `$${(v / 1e3).toFixed(1)}K`;
  if (v < 10) return `$${v.toFixed(1)}`;
  return `$${v.toFixed(0)}`;
};
const fNc = (v: number): string => {
  if (v == null || isNaN(v) || !isFinite(v)) return "0";
  if (v >= 1e9) return `${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return `${v}`;
};
const fN = (v: number): string => {
  if (v == null || isNaN(v) || !isFinite(v)) return "0";
  if (v >= 1e9) return `${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
  return `${v}`;
};
const pD = (a: number | null | undefined, b: number | null | undefined): number => {
  if (b == null || b === 0 || a == null) return 0;
  const r = (a / b - 1) * 100;
  if (!isFinite(r) || isNaN(r)) return 0;
  return r;
};
const safe = (v: number | null | undefined, fallback = 0): number => (v == null || isNaN(v)) ? fallback : v;

// ── Simulated Fallback Data ───────────────────────────────────────────────
const SIM_STATES: Record<string, StateData> = {
  FL:{name:"Florida",spend:32.8e9,enroll:3624248,pe:9050,fmap:58.6,mc:77,provs:68500,em:12800,hcbs:14200,bh:5800,dn:6400,pi:0.87,mi:0.95},
  NY:{name:"New York",spend:91.2e9,enroll:5952946,pe:15319,fmap:50,mc:76,provs:142e3,em:28200,hcbs:35e3,bh:14200,dn:14800,pi:1.30,mi:1.15},
  TX:{name:"Texas",spend:47.2e9,enroll:3833095,pe:12313,fmap:61.8,mc:73,provs:78400,em:14800,hcbs:15200,bh:6200,dn:7800,pi:0.94,mi:1.02},
  CA:{name:"California",spend:140.5e9,enroll:12175605,pe:11540,fmap:50,mc:82,provs:189e3,em:38200,hcbs:42e3,bh:18500,dn:19800,pi:1.15,mi:1.08},
  PA:{name:"Pennsylvania",spend:37.6e9,enroll:2781015,pe:13522,fmap:52.3,mc:78,provs:52400,em:10200,hcbs:12400,bh:5400,dn:5600,pi:1.05,mi:1.04},
  OH:{name:"Ohio",spend:28.5e9,enroll:2632166,pe:10828,fmap:62.4,mc:84,provs:48600,em:9400,hcbs:10200,bh:4800,dn:5e3,pi:0.95,mi:0.98},
  IL:{name:"Illinois",spend:24.7e9,enroll:2936525,pe:8412,fmap:50.9,mc:65,provs:48200,em:9200,hcbs:10800,bh:4600,dn:5100,pi:0.98,mi:0.93},
  GA:{name:"Georgia",spend:14.5e9,enroll:1700970,pe:8525,fmap:67,mc:72,provs:28900,em:5400,hcbs:5200,bh:2200,dn:2800,pi:0.84,mi:0.88},
  MN:{name:"Minnesota",spend:16.8e9,enroll:1152907,pe:14572,fmap:50,mc:72,provs:26400,em:5200,hcbs:7200,bh:3800,dn:2800,pi:1.10,mi:1.12},
  AZ:{name:"Arizona",spend:16.2e9,enroll:1810967,pe:8946,fmap:70.8,mc:85,provs:22400,em:4100,hcbs:5800,bh:2400,dn:2100,pi:0.92,mi:0.96},
  MA:{name:"Massachusetts",spend:21.8e9,enroll:1455521,pe:14979,fmap:50,mc:72,provs:38500,em:7800,hcbs:8200,bh:4200,dn:3800,pi:1.18,mi:1.06},
  MI:{name:"Michigan",spend:19.6e9,enroll:2188259,pe:8956,fmap:65.6,mc:78,provs:38200,em:7200,hcbs:8100,bh:3600,dn:3900,pi:0.93,mi:0.97},
  NC:{name:"N. Carolina",spend:19.3e9,enroll:2451104,pe:7875,fmap:66.7,mc:75,provs:32100,em:6200,hcbs:6400,bh:2800,dn:3200,pi:0.91,mi:0.94},
  WA:{name:"Washington",spend:16.1e9,enroll:1776840,pe:9061,fmap:50,mc:85,provs:28900,em:5800,hcbs:6800,bh:3200,dn:3100,pi:1.08,mi:0.97},
  CO:{name:"Colorado",spend:12.1e9,enroll:1060589,pe:11409,fmap:50,mc:70,provs:21500,em:4800,hcbs:4200,bh:2600,dn:2200,pi:1.00,mi:0.96},
  NJ:{name:"New Jersey",spend:20.1e9,enroll:1509804,pe:13313,fmap:50,mc:82,provs:35600,em:6800,hcbs:7800,bh:3400,dn:3600,pi:1.05,mi:1.01},
  MD:{name:"Maryland",spend:14.5e9,enroll:1312635,pe:11046,fmap:50,mc:78,provs:24600,em:4800,hcbs:5600,bh:2600,dn:2600,pi:1.03,mi:0.99},
  VA:{name:"Virginia",spend:14.2e9,enroll:1605016,pe:8847,fmap:50,mc:82,provs:28400,em:5600,hcbs:5800,bh:2600,dn:2900,pi:0.93,mi:0.98},
  IN:{name:"Indiana",spend:15.3e9,enroll:1626670,pe:9406,fmap:66.9,mc:78,provs:24300,em:4800,hcbs:5100,bh:2400,dn:2500,pi:0.90,mi:0.99},
  OR:{name:"Oregon",spend:13.2e9,enroll:1119407,pe:11792,fmap:60.1,mc:88,provs:18900,em:3800,hcbs:4600,bh:2400,dn:2e3,pi:1.05,mi:1.01}
};
const SIM_HC: HcpcsCode[] = [
  {c:"99213",d:"Office Visit (Low)",cat:"E&M",na:48.52,r:{FL:42.18,NY:62.4,TX:45.8,CA:55.9,PA:51.2,OH:46.3,GA:40.5,MN:52.1,AZ:44.6,MA:58.2,IL:47.8,MI:45.1,NC:43.8,WA:52.4,CO:49.2,NJ:50.8,MD:50.1,VA:44.9,IN:43.6,OR:50.8},nc:48.2e6},
  {c:"99214",d:"Office Visit (Mod)",cat:"E&M",na:74.6,r:{FL:64.8,NY:95.2,TX:70.1,CA:86.5,PA:78.4,OH:71.2,GA:62.8,MN:80.3,AZ:68.4,MA:89.8,IL:73.1,MI:69.4,NC:67.2,WA:80.8,CO:75.6,NJ:78.2,MD:77.2,VA:69,IN:67,OR:78.4},nc:38.6e6},
  {c:"T1019",d:"Personal Care /15m",cat:"HCBS/Waiver",na:5.82,r:{FL:4.95,NY:8.4,TX:5.2,CA:7.1,PA:6.2,OH:5.6,GA:4.8,MN:7.8,AZ:6.9,MA:7.2,IL:5.4,MI:5.3,NC:4.6,WA:7.5,CO:6.8,NJ:6.4,MD:6.1,VA:5.5,IN:5.1,OR:7.4},nc:2.1e9,tr:[{y:2018,v:3.8},{y:2019,v:4.1},{y:2020,v:4.6},{y:2021,v:5.2},{y:2022,v:5.9},{y:2023,v:6.4},{y:2024,v:7.1}],cn:{t1:42.5,t5:68.2,t10:81.4,gi:0.89}},
  {c:"T2025",d:"Waiver Svcs NOS",cat:"HCBS/Waiver",na:42.1,r:{FL:35.8,NY:58.6,TX:38.4,CA:48.9,PA:44.2,OH:40.1,GA:34.2,MN:48.5,AZ:52.4,MA:50.2,IL:41.3,MI:38.8,NC:36.4,WA:46.8,CO:44.6,NJ:44.8,MD:42.6,VA:38.2,IN:36.8,OR:48.2},nc:114e6,tr:[{y:2018,v:28},{y:2019,v:31},{y:2020,v:34},{y:2021,v:38},{y:2022,v:42},{y:2023,v:46},{y:2024,v:51}],cn:{t1:38.2,t5:62.8,t10:76.1,gi:0.85}},
  {c:"97153",d:"ABA Therapy",cat:"Behavioral",na:28.4,r:{FL:24.6,NY:35.8,TX:26.8,CA:32.5,PA:29.8,OH:27.1,GA:23.8,MN:38.2,AZ:30.4,MA:33.8,IL:27.8,MI:26.2,NC:25.4,WA:31.6,CO:29,NJ:31.2,MD:28.8,VA:26,IN:25.2,OR:30.8},nc:45.2e6,tr:[{y:2018,v:18},{y:2019,v:22},{y:2020,v:24},{y:2021,v:28},{y:2022,v:34},{y:2023,v:42},{y:2024,v:52}],cn:{t1:31.7,t5:54.3,t10:68.9,gi:0.82}},
  {c:"90834",d:"Psychotherapy 45m",cat:"Behavioral",na:82.6,r:{FL:71.8,NY:108.4,TX:77.6,CA:95.8,PA:86.8,OH:78.8,GA:69.4,MN:89.2,AZ:76.4,MA:99.6,IL:80.9,MI:77,NC:74.2,WA:89.2,CO:83.6,NJ:86.4,MD:85.4,VA:76.4,IN:74,OR:86.8},nc:28.4e6},
  {c:"D0120",d:"Periodic Oral Eval",cat:"Dental",na:32.4,r:{FL:28.6,NY:42.8,TX:30.2,CA:38.4,PA:34.8,OH:31.4,GA:27.6,MN:36.2,AZ:31.8,MA:38.6,IL:32.1,MI:30.4,NC:29.4,WA:35.8,CO:33.2,NJ:34.8,MD:33.6,VA:30.2,IN:29.8,OR:34.2},nc:22.1e6},
  {c:"59400",d:"OB Care Vaginal",cat:"Maternity",na:2145,r:{FL:1890,NY:2820,TX:1960,CA:2480,PA:2240,OH:2050,GA:1820,MN:2380,AZ:2020,MA:2580,IL:2120,MI:1980,NC:1920,WA:2340,CO:2180,NJ:2280,MD:2200,VA:1960,IN:1880,OR:2280},nc:1.8e6},
  {c:"J3490",d:"Unclassified Drugs",cat:"Drugs",na:185,r:{FL:162,NY:242,TX:174,CA:215,PA:196,OH:178,GA:156,MN:205,AZ:180,MA:218,IL:182,MI:172,NC:168,WA:202,CO:190,NJ:198,MD:192,VA:174,IN:170,OR:198},nc:52.4e6},
  {c:"91124",d:"Esophageal Motility",cat:"Diagnostic",na:188,r:{FL:164,NY:248,TX:178,CA:220,PA:198,OH:182,GA:158,MN:210,AZ:184,MA:224,IL:186,MI:176,NC:170,WA:206,CO:192,NJ:202,MD:196,VA:176,IN:172,OR:202},nc:420e3}
];
const SIM_NATL: NatlTrend[] = [
  {y:2018,s:597,e:72.4,pe:8243},{y:2019,s:613,e:71.5,pe:8573},
  {y:2020,s:671,e:76.8,pe:8737},{y:2021,s:734,e:83.4,pe:8802},
  {y:2022,s:805,e:90.8,pe:8865},{y:2023,s:849,e:89.6,pe:9475},
  {y:2024,s:862,e:83.2,pe:10360}
];

const STATE_NAMES: Record<string, string> = {AL:"Alabama",AK:"Alaska",AZ:"Arizona",AR:"Arkansas",CA:"California",CO:"Colorado",CT:"Connecticut",DE:"Delaware",DC:"D.C.",FL:"Florida",GA:"Georgia",HI:"Hawaii",ID:"Idaho",IL:"Illinois",IN:"Indiana",IA:"Iowa",KS:"Kansas",KY:"Kentucky",LA:"Louisiana",ME:"Maine",MD:"Maryland",MA:"Massachusetts",MI:"Michigan",MN:"Minnesota",MS:"Mississippi",MO:"Missouri",MT:"Montana",NE:"Nebraska",NV:"Nevada",NH:"New Hampshire",NJ:"New Jersey",NM:"New Mexico",NY:"New York",NC:"N. Carolina",ND:"N. Dakota",OH:"Ohio",OK:"Oklahoma",OR:"Oregon",PA:"Pennsylvania",RI:"Rhode Island",SC:"S. Carolina",SD:"S. Dakota",TN:"Tennessee",TX:"Texas",UT:"Utah",VT:"Vermont",VA:"Virginia",WA:"Washington",WV:"W. Virginia",WI:"Wisconsin",WY:"Wyoming",PR:"Puerto Rico",GU:"Guam",VI:"Virgin Islands",AS:"American Samoa",MP:"N. Mariana Is."};

// FFS share by state (1 - managed care penetration). Source: KFF/CMS 2023.
const FFS_SHARE: Record<string, number> = {AK:0.78,AL:0.28,AR:0.30,AZ:0.15,CA:0.18,CO:0.30,CT:0.22,DC:0.25,DE:0.33,FL:0.23,GA:0.28,GU:1.0,HI:0.22,IA:0.18,ID:0.38,IL:0.35,IN:0.22,KS:0.22,KY:0.17,LA:0.22,MA:0.28,MD:0.22,ME:0.35,MI:0.22,MN:0.28,MO:0.28,MS:0.30,MT:0.55,NC:0.25,ND:0.38,NE:0.18,NH:0.30,NJ:0.18,NM:0.18,NV:0.22,NY:0.24,OH:0.16,OK:0.35,OR:0.12,PA:0.22,PR:0.35,RI:0.22,SC:0.28,SD:0.48,TN:0.12,TX:0.27,UT:0.30,VA:0.18,VI:1.0,VT:0.40,WA:0.15,WI:0.22,WV:0.30,WY:0.58,AS:1.0,MP:1.0};
// ── Data transformation helpers ───────────────────────────────────────────
function transformStates(raw: RawState[] | null): Record<string, StateData> {
  if (!raw || !Array.isArray(raw) || raw.length === 0) return SIM_STATES;
  const out: Record<string, StateData> = {};
  for (const s of raw) {
    if (!s.state) continue;
    out[s.state] = {
      name: STATE_NAMES[s.state] || s.state,
      spend: safe(s.total_spend), enroll: safe(s.est_enrollment),
      pe: safe(s.per_enrollee), fmap: safe(s.fmap, 50),
      mc: Math.round((1 - (FFS_SHARE[s.state] || 0.40)) * 100),
      provs: safe(s.n_providers), em: safe(s.em_provs),
      hcbs: safe(s.hcbs_provs), bh: safe(s.bh_provs), dn: safe(s.dental_provs),
      pi: safe(s.price_index, 1), mi: safe(s.mix_index, 1)
    };
  }
  return Object.keys(out).length > 0 ? out : SIM_STATES;
}

function transformHcpcs(raw: RawHcpcs[] | null): HcpcsCode[] {
  if (!raw || !Array.isArray(raw) || raw.length === 0) return SIM_HC;
  return raw.map(h => ({
    c: h.code || "?", d: h.desc || h.code || "Unknown", cat: h.category || "Other",
    na: safe(h.national_avg), nc: safe(h.national_claims), ns: safe(h.national_spend),
    nst: safe(h.n_states), np: safe(h.n_providers),
    r: h.rates || {},
    tr: h.trend ? h.trend.map(t => ({ y: t.year, v: safe(t.avg_rate) })) : null,
    cn: h.concentration ? {
      t1: safe(h.concentration.top1_pct), t5: safe(h.concentration.top5_pct),
      t10: safe(h.concentration.top10_pct), gi: safe(h.concentration.gini)
    } : null
  }));
}

function transformTrends(raw: RawTrend[] | null): NatlTrend[] {
  if (!raw || !Array.isArray(raw) || raw.length === 0) return SIM_NATL;
  // Known CMS enrollment milestones (millions) - from published CMS data
  const ENROLL: Record<number, number> = {2018:72.4,2019:71.5,2020:76.8,2021:83.4,2022:90.8,2023:89.6,2024:83.2};
  return raw.map(t => {
    const y = t.year;
    const s = safe(t.total_spend) / 1e9;
    const bene = safe(t.total_bene);
    // Pipeline total_bene may be beneficiary-service records (~1.8B), not unique enrollees (~83M).
    // Only trust it if it falls in a plausible Medicaid enrollment range (50M–200M).
    const beneM = bene / 1e6;
    const e = (beneM >= 50 && beneM <= 200) ? beneM : (ENROLL[y] || 83);
    return { y, s, e, pe: e > 0 ? (s * 1e9) / (e * 1e6) : 0 };
  });
}

// ── UI Components ────────────────────────────────────────────────────────
function ChartModal({ children, onClose }: { children: React.ReactNode; onClose: () => void }) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);
  return (
    <div onClick={onClose} style={{ position:"fixed",inset:0,zIndex:9999,background:"rgba(10,37,64,0.6)",backdropFilter:"blur(4px)",display:"flex",alignItems:"center",justifyContent:"center",padding:16 }}>
      <div onClick={e=>e.stopPropagation()} style={{ background:WH,borderRadius:14,boxShadow:"0 24px 80px rgba(0,0,0,0.3)",width:"100%",maxWidth:900,maxHeight:"90vh",overflow:"auto",position:"relative" }}>
        <button onClick={onClose} style={{ position:"absolute",top:8,right:12,background:"none",border:"none",fontSize:18,color:AL,cursor:"pointer",zIndex:1,lineHeight:1 }}>&times;</button>
        <style>{`.xm [style*="maxHeight"]{max-height:none !important;overflow:visible !important;}.xm [style*="overflowY"]{overflow:visible !important;}`}</style>
        <div className="xm" style={{ padding:"12px 16px" }}>{children}</div>
      </div>
    </div>
  );
}

function Card({ children, accent, x }: { children: React.ReactNode; accent?: string; x?: boolean }) {
  const [open, setOpen] = useState(false);
  return (<>
    <div style={{ background: WH, borderRadius: 10, boxShadow: SH, overflow: "hidden",
      borderTop: accent ? `3px solid ${accent}` : "none", position: x ? "relative" : undefined }}>
      {children}
      {x && <button onClick={()=>setOpen(true)} style={{ position:"absolute",top:8,right:10,background:"rgba(255,255,255,0.85)",border:"none",borderRadius:4,cursor:"pointer",color:AL,fontSize:13,padding:"0 3px",opacity:0.35,lineHeight:1,zIndex:1 }} title="Expand">⤢</button>}
    </div>
    {open && <ChartModal onClose={()=>setOpen(false)}>{children}</ChartModal>}
  </>);
}

const CH = ({ t, b, r }: { t: string; b?: React.ReactNode; r?: string }) => (
  <div style={{ padding: "10px 14px 4px", display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
    <div>
      <span style={{ fontSize: 11, fontWeight: 700, color: A }}>{t}</span>
      {b && <span style={{ fontSize: 9, color: AL, marginLeft: 6 }}>{b}</span>}
    </div>
    {r && <span style={{ fontSize: 9, color: AL, fontFamily: FM }}>{r}</span>}
  </div>
);

const Met = ({ l, v, cl }: { l: string; v: React.ReactNode; cl?: string }) => (
  <div style={{ textAlign: "center", padding: "3px 2px" }}>
    <div style={{ fontSize: 8, color: AL, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 1 }}>{l}</div>
    <div style={{ fontFamily: FM, fontSize: 13, fontWeight: 600, color: cl || A }}>{v}</div>
  </div>
);

const Pill = ({ children, on, onClick }: { children: React.ReactNode; on: boolean; onClick: () => void }) => (
  <button onClick={onClick} style={{
    padding: "3px 10px", borderRadius: 20, border: `1px solid ${on ? cB : B}`,
    background: on ? "rgba(46,107,74,0.07)" : WH, color: on ? cB : AL,
    fontSize: 10, fontWeight: on ? 600 : 400, cursor: "pointer", whiteSpace: "nowrap"
  }}>{children}</button>
);

const Bdg = ({ children }: { children: React.ReactNode }) => (
  <span style={{ fontSize: 8, padding: "1px 6px", borderRadius: 8,
    background: "rgba(46,107,74,0.06)", color: cB, fontWeight: 600, whiteSpace: "nowrap"
  }}>{children}</span>
);

const TabGuide = ({ title, desc, tips }: { title: string; desc: string; tips?: string }) => (
  <div style={{ background: `${cB}08`, border: `1px solid ${cB}18`, borderRadius: 10, padding: "10px 16px", marginBottom: 2 }}>
    <div style={{ fontSize: 12, fontWeight: 600, color: A, marginBottom: 3 }}>{title}</div>
    <div style={{ fontSize: 11, color: AL, lineHeight: 1.6 }}>{desc}</div>
    {tips && <div style={{ fontSize: 9, color: AL, marginTop: 4, fontStyle: "italic" }}>{tips}</div>}
  </div>
);

const ExportBtn = ({ onClick, label }: { onClick: () => void; label?: string }) => (
  <button onClick={onClick} style={{ fontSize: 9, color: cB, background: WH, border: `1px solid ${B}`, borderRadius: 5, padding: "4px 10px", cursor: "pointer", whiteSpace: "nowrap", fontWeight: 600, display: "inline-flex", alignItems: "center", gap: 3 }}>
    <span style={{ fontSize: 10 }}>↓</span> {label || "Export CSV"}
  </button>
);

function downloadCSV(filename: string, headers: string[], rows: (string | number)[][]) {
  const esc = (v: string | number) => typeof v === "string" && (v.includes(",") || v.includes('"')) ? `"${v.replace(/"/g, '""')}"` : String(v ?? "");
  const csv = [headers.join(","), ...rows.map(r => r.map(esc).join(","))].join("\n");
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
  a.download = filename;
  a.click();
}

// Safe tooltip for Recharts
function SafeTip({ active, payload, render }: SafeTipProps) {
  if (!active || !payload || !payload[0] || !payload[0].payload) return null;
  try {
    return (
      <div style={{ background: WH, border: `1px solid ${B}`, borderRadius: 6,
        padding: "6px 10px", fontSize: 10, boxShadow: SH }}>
        {render(payload[0].payload)}
      </div>
    );
  } catch (_e: unknown) { return null; }
}

// ── Hex Map ─────────────────────────────────────────────────────────────
const HEX_POS: Record<string, [number, number]> = {
  ME:[10,0],VT:[9,1],NH:[10,1],WA:[1,2],MT:[2,2],ND:[3,2],MN:[4,2],WI:[5,2],MI:[7,2],NY:[8,2],MA:[9,2],CT:[10,2],
  OR:[1,3],ID:[2,3],SD:[3,3],IA:[4,3],IL:[5,3],IN:[6,3],OH:[7,3],PA:[8,3],NJ:[9,3],RI:[10,3],
  CA:[0,4],NV:[1,4],WY:[2,4],NE:[3,4],MO:[4,4],KY:[5,4],WV:[6,4],VA:[7,4],MD:[8,4],DE:[9,4],DC:[10,4],
  AZ:[1,5],UT:[2,5],CO:[3,5],KS:[4,5],AR:[5,5],TN:[6,5],NC:[7,5],SC:[8,5],
  NM:[2,6],OK:[4,6],LA:[5,6],MS:[6,6],AL:[7,6],GA:[8,6],HI:[1,7],TX:[4,7],FL:[8,7],AK:[0,7]
};

function HexMap({ states, fn, fmt, onSel, sel }: { states: Record<string, StateData>; fn: (s: StateData) => number; fmt: (v: number) => string; onSel: (k: string) => void; sel: string }) {
  const keys = Object.keys(states).filter(k => HEX_POS[k]);
  const territories = Object.keys(states).filter(k => !HEX_POS[k] && k !== "US");
  if (keys.length === 0 && territories.length === 0) return null;
  const vals = [...keys, ...territories].map(k => safe(fn(states[k])));
  const mx = Math.max(...vals, 1);
  return (
    <div>
      <svg viewBox="0 0 346 244" style={{ width: "100%" }}>
        {keys.map(k => {
          const pos = HEX_POS[k];
          const x = pos[0] * 30 + ((pos[1] % 2) ? 15 : 0);
          const y = pos[1] * 28;
          const v = safe(fn(states[k]));
          const pct = mx > 0 ? v / mx : 0;
          const isSel = k === sel;
          return (
            <g key={k} onClick={() => onSel(k)} style={{ cursor: "pointer" }}>
              <rect x={x} y={y} width={26} height={24} rx={4}
                fill={isSel ? A : `rgba(46,107,74,${(0.12 + pct * 0.68).toFixed(2)})`}
                stroke={isSel ? cO : "none"} strokeWidth={isSel ? 2 : 0} />
              <text x={x + 13} y={y + 10} textAnchor="middle"
                fill={isSel || pct > 0.5 ? WH : A} fontSize={7} fontWeight={700} fontFamily={FM}>{k}</text>
              <text x={x + 13} y={y + 20} textAnchor="middle"
                fill={isSel ? "rgba(255,255,255,0.7)" : AL} fontSize={5} fontFamily={FM}>{fmt(v)}</text>
            </g>
          );
        })}
      </svg>
      {territories.length > 0 && <div style={{ display:"flex",gap:4,flexWrap:"wrap",padding:"2px 0" }}>
        {territories.map(k => {
          const v = safe(fn(states[k]));
          const isSel = k === sel;
          return <div key={k} onClick={()=>onSel(k)} style={{ cursor:"pointer",padding:"2px 6px",borderRadius:4,background:isSel?A:"rgba(46,107,74,0.08)",border:isSel?`1px solid ${cO}`:`1px solid ${B}`,fontSize:8,fontFamily:FM,display:"flex",gap:4,alignItems:"center" }}>
            <span style={{ fontWeight:700,color:isSel?WH:A }}>{k}</span>
            <span style={{ color:isSel?"rgba(255,255,255,0.7)":AL }}>{fmt(v)}</span>
          </div>;
        })}
      </div>}
    </div>
  );
}

// ── Code Search ─────────────────────────────────────────────────────────
function CodeSearch({ codes, value, onChange, maxShow = 50 }: { codes: HcpcsCode[]; value: string | null; onChange: (v: string) => void; maxShow?: number }) {
  const [sq, setSQ] = useState("");
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const filtered = useMemo(() => {
    if (!sq) return [];
    return expandSearch(sq, codes).slice(0, maxShow);
  }, [codes, sq, maxShow]);
  const hasSyn = useMemo(() => {
    if (!sq) return false;
    const lq = sq.toLowerCase().trim();
    if (lq.length < 2) return false;
    return Object.keys(SYNONYMS).some(t => t.includes(lq) || lq.includes(t));
  }, [sq]);
  const current = codes.find(c => c.c === value);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => { if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  return (
    <div ref={ref} style={{ position: "relative", maxWidth: 420, flex: 1 }}>
      <div style={{ position: "relative" }}>
        <input value={sq} onChange={e => { setSQ(e.currentTarget.value); setOpen(true); }}
          onFocus={() => { if (sq) setOpen(true); }}
          placeholder={current ? `${current.c} — ${current.d.substring(0,40)}` : "Search by code, name, or category..."}
          style={{ width: "100%", background: S, border: `1px solid ${B}`,
            padding: "7px 10px 7px 26px", borderRadius: 6, fontSize: 11,
            outline: "none", boxSizing: "border-box", fontFamily: FM }} />
        <span style={{ position: "absolute", left: 8, top: "50%",
          transform: "translateY(-50%)", color: AL, fontSize: 12 }}>&#x2315;</span>
      </div>
      {open && sq && filtered.length > 0 && <div style={{
        position: "absolute", top: "100%", left: 0, right: 0, zIndex: 999,
        background: "#fff", border: `1px solid ${B}`, borderRadius: 6,
        maxHeight: 280, overflowY: "auto", boxShadow: "0 4px 12px rgba(0,0,0,0.1)",
        marginTop: 2
      }}>
        <div style={{ fontSize: 9, color: AL, padding: "4px 10px", borderBottom: `1px solid ${B}` }}>
          {filtered.length} match{filtered.length !== 1 ? "es" : ""}{hasSyn ? " (includes related)" : ""}{filtered.length >= maxShow ? ` · first ${maxShow}` : ""}
        </div>
        {filtered.map(x => (
          <div key={x.c} onClick={() => { onChange(x.c); setSQ(""); setOpen(false); }}
            style={{ padding: "5px 10px", cursor: "pointer", fontSize: 11, display: "flex",
              gap: 6, alignItems: "center", borderBottom: `1px solid ${B}`,
              background: x.c === value ? "rgba(26,74,51,0.04)" : "transparent" }}
            onMouseEnter={e => e.currentTarget.style.background = "rgba(26,74,51,0.06)"}
            onMouseLeave={e => e.currentTarget.style.background = x.c === value ? "rgba(26,74,51,0.04)" : "transparent"}>
            <span style={{ fontFamily: FM, fontWeight: 600, minWidth: 48 }}>{x.c}</span>
            <span style={{ color: AL, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{x.d}</span>
            <span style={{ fontSize: 9, color: AL, opacity: 0.6 }}>{x.cat}</span>
          </div>
        ))}
      </div>}
      {open && sq && filtered.length === 0 && <div style={{
        position: "absolute", top: "100%", left: 0, right: 0, zIndex: 999,
        background: "#fff", border: `1px solid ${B}`, borderRadius: 6, padding: "8px 10px",
        fontSize: 10, color: AL, marginTop: 2, boxShadow: "0 4px 12px rgba(0,0,0,0.1)"
      }}>No codes match "{sq}"</div>}
    </div>
  );
}

// ── Main App ──────────────────────────────────────────────────────────────
export default function TmsisExplorer() {
  const [tab, setTab] = useState("dash");
  const [s1, setS1] = useState("FL");
  const [s2, setS2] = useState("");
  const [s3, setS3] = useState("");
  const [mm, setMM] = useState("pe");
  const [q, setQ] = useState("");
  const [bCat, setBC] = useState("All");
  const [bSort, setBS] = useState("fiscal");
  const [mixAdj, setMixAdj] = useState(false);
  const [dc, setDC] = useState<string | null>(null);
  const [ac, setAC] = useState("em");
  const [insightOpen, setIO] = useState<string | null>(null);
  const [showAllIns, setSAI] = useState(false);
  const [pq, setPQ] = useState("");
  const [selNpi, setSelNpi] = useState<string | null>(null); // separate selection from search
  const [provMode, setPM] = useState("providers"); // "providers" or "specialties"
  const [specQuery, setSpecQuery] = useState("");
  const [selSpecTax, setSelSpecTax] = useState<string | null>(null);
  const [simCat, setSimCat] = useState("All");
  const [simPct, setSimPct] = useState(10);
  const [simState, setSimSt] = useState("FL");

  const [meta, setMeta] = useState<PipelineMeta | null>(null);
  const [states, setStates] = useState<Record<string, StateData>>(SIM_STATES);
  const [codes, setCodes] = useState<HcpcsCode[]>(SIM_HC);
  const [trends, setTrends] = useState<NatlTrend[]>(SIM_NATL);
  const [regions, setRegions] = useState<Record<string, unknown> | null>(null);
  const [providerData, setPD] = useState<ProviderRecord[] | null>(null);
  const [specData, setSpec] = useState<SpecialtyRecord[] | null>(null);
  const [loading, setLoading] = useState(true);

  // Reference data: Medicare rates, risk adjustment, fee schedules
  const [mcRates, setMCR] = useState<MedicareRates | null>(null);
  const [riskAdj, setRA] = useState<RiskAdjData | null>(null);
  const [feeScheds, setFS] = useState<FeeScheduleData | null>(null);
  const [fsDir, setFSD] = useState<FeeScheduleDirectory | null>(null);
  const [peMode, setPEM] = useState("raw");    // "raw" or "adj" (risk-adjusted per enrollee)

  const isLive = meta?.live === true;

  // Load pipeline data
  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const metaRes = await fetch("/data/meta.json");
        if (!metaRes.ok) throw new Error("No pipeline data");
        const m = await metaRes.json();
        if (cancelled) return;
        setMeta(m);
        const [statesRaw, hcpcsRaw, trendsRaw, mcrRaw, suppDesc] = await Promise.all([
          fetch("/data/states.json").then(r => r.json()).catch(() => null),
          fetch("/data/hcpcs.json").then(r => r.json()).catch(() => null),
          fetch("/data/trends.json").then(r => r.json()).catch(() => null),
          fetch("/data/medicare_rates.json").then(r => r.json()).catch(() => null),
          fetch("/data/hcpcs_descriptions.json").then(r => r.json()).catch(() => null)
        ]);
        if (cancelled) return;
        if (mcrRaw?.rates) setMCR(mcrRaw);
        setStates(transformStates(statesRaw));
        // Enrich codes with descriptions where pipeline has none
        if (hcpcsRaw) {
          for (const h of hcpcsRaw) {
            if (!h.desc || h.desc === h.code) {
              h.desc = mcrRaw?.rates?.[h.code]?.d || suppDesc?.[h.code] || h.desc;
            }
          }
        }
        setCodes(transformHcpcs(hcpcsRaw));
        if (trendsRaw) setTrends(transformTrends(trendsRaw));
        try {
          const regRaw = await fetch("/data/regions.json").then(r => r.json());
          if (!cancelled) setRegions(regRaw);
        } catch (_e: unknown) {}
        try {
          const provRaw = await fetch("/data/providers.json").then(r => r.json());
          if (!cancelled && Array.isArray(provRaw)) setPD(provRaw);
        } catch (_e: unknown) {}
        try {
          const specRaw = await fetch("/data/specialties.json").then(r => r.json());
          if (!cancelled && Array.isArray(specRaw)) setSpec(specRaw);
        } catch (_e: unknown) {}
        // Reference data (optional — enhance display when available)
        try {
          const ra = await fetch("/data/risk_adj.json").then(r => r.json());
          if (!cancelled && ra?.states) setRA(ra);
        } catch (_e: unknown) {}
        try {
          const fs = await fetch("/data/fee_schedules.json").then(r => r.json());
          if (!cancelled && fs?.states) setFS(fs);
        } catch (_e: unknown) {}
        try {
          const fsd = await fetch("/data/fee_schedule_directory.json").then(r => r.json());
          if (!cancelled && fsd?.directory) setFSD(fsd);
        } catch (_e: unknown) {}
      } catch (_e: unknown) {
        if (!cancelled) setMeta({ live: false, source: "simulated" });
      }
      if (!cancelled) setLoading(false);
    }
    load();
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (codes.length > 0 && !dc) setDC(codes[0].c);
  }, [codes, dc]);

  const SL = useMemo(() => Object.keys(states).filter(k => k !== "US").sort((a, b) =>
    (states[a]?.name || a).localeCompare(states[b]?.name || b)
  ), [states]);

  const emptyState: StateData = { name: "—", spend: 0, enroll: 0, pe: 0, fmap: 50, mc: 0, provs: 0, em: 0, hcbs: 0, bh: 0, dn: 0 };
  const g = useCallback((k: string): StateData => states[k] || { ...emptyState, name: k }, [states]);
  const CATS = useMemo(() => [...new Set(codes.map(h => h.cat))].sort(), [codes]);

  const ACC = [{ k:"em",l:"E&M",fn:(s: StateData)=>safe(s?.em) },{ k:"hcbs",l:"HCBS",fn:(s: StateData)=>safe(s?.hcbs) },{ k:"bh",l:"Behavioral",fn:(s: StateData)=>safe(s?.bh) },{ k:"dn",l:"Dental",fn:(s: StateData)=>safe(s?.dn) }];

  // Reference data helpers
  const getMcRate = useCallback((code: string) => mcRates?.rates?.[code]?.r || 0, [mcRates]);
  const getFsRate = useCallback((state: string, code: string) => { const e = feeScheds?.states?.[state]?.rates?.[code]; return typeof e === "number" ? e : (e && typeof e === "object" && "r" in e ? e.r : 0); }, [feeScheds]);
  const getAdj = useCallback((state: string) => riskAdj?.states?.[state]?.factor || 1.0, [riskAdj]);
  const hasRef = mcRates || feeScheds;
  const hasAdj = !!riskAdj;

  const mms: Record<string, { l: string; fn: (s: StateData) => number; f: (v: number) => string; fc?: (v: number) => string }> = {
    pe: { l: "Per Cap", fn: (s: StateData) => {
      // In adjusted mode, divide by eligibility-mix factor
      const raw = safe(s?.pe);
      if (peMode === "adj" && hasAdj) {
        // Find state key — s is a state object with name property
        const sk = Object.entries(states).find(([_k,v]) => v === s)?.[0];
        if (sk) { const f = getAdj(sk); return f > 0 ? raw / f : raw; }
      }
      return raw;
    }, f: (v: number) => `$${safe(v).toLocaleString(undefined,{maximumFractionDigits:0})}`, fc: f$c },
    s: { l: "Spend", fn: (s: StateData) => safe(s?.spend), f: f$, fc: f$c },
    e: { l: "Enroll", fn: (s: StateData) => safe(s?.enroll), f: fN, fc: fNc },
    pv: { l: "Provs", fn: (s: StateData) => safe(s?.provs), f: fN, fc: fNc },
    ac: { l: "Access", fn: (s: StateData) => { const sc = ACC.find(x=>x.k===ac)||ACC[0]; const cp=sc.fn(s); return safe(s?.enroll)>0?cp/(safe(s?.enroll)/1e3):0; }, f: (v: number) => `${safe(v).toFixed(1)}/1K`, fc: (v: number) => `${safe(v).toFixed(0)}` }
  };
  const curM = mms[mm] || mms.pe;
  const d1 = g(s1);
  const natlPE = useMemo(() => {
    const totalSpend = SL.reduce((sum, k) => sum + safe(states[k]?.spend), 0);
    const totalEnroll = SL.reduce((sum, k) => sum + safe(states[k]?.enroll), 0);
    if (totalEnroll > 0 && totalSpend > 0) return totalSpend / totalEnroll;
    return trends.length > 0 ? safe(trends[trends.length - 1].pe, 9475) : 9475;
  }, [SL, states, trends]);
  const d1PE = useMemo(() => { const raw = safe(d1.pe); if (peMode==="adj"&&hasAdj) { const f=getAdj(s1); return f>0?raw/f:raw; } return raw; }, [d1, peMode, hasAdj, getAdj, s1]);
  const natlPEAdj = useMemo(() => { if (peMode==="adj"&&hasAdj) { const totalAdj=SL.reduce((a,k)=>{ const f=getAdj(k); const raw=safe(states[k]?.pe); return a+(f>0?raw/f:raw)*safe(states[k]?.enroll); },0); const totalEnr=SL.reduce((a,k)=>a+safe(states[k]?.enroll),0); return totalEnr>0?totalAdj/totalEnr:natlPE; } return natlPE; }, [SL, states, natlPE, peMode, hasAdj, getAdj]);
  const d1Dev = pD(d1PE, natlPEAdj);

  // ── Computed Insights ──────────────────────────────────────────────────
  const insights = useMemo(() => {
    const ins = [];
    const sortedPE = SL.map(k=>({k,n:states[k]?.name||k,pe:safe(states[k]?.pe)})).sort((a,b)=>b.pe-a.pe);
    const topPE = sortedPE.slice(0,5);
    const botPE = [...sortedPE].reverse().slice(0,5);
    if (topPE.length > 2) {
      ins.push({
        id:"spend_top", q:"Which states spend the most per person?",
        a:`${topPE[0].n} leads at ${f$(topPE[0].pe)} per enrollee, ${((topPE[0].pe/natlPE-1)*100).toFixed(0)}% above the national average of ${f$(natlPE)}. ${topPE[1].n} and ${topPE[2].n} round out the top three.`,
        data:topPE.map(s=>({n:s.n.substring(0,12),v:s.pe})), color:cO, unit:"$",
        action:()=>{setMM("pe");}
      });
      ins.push({
        id:"spend_bot", q:"Which states spend the least per enrollee?",
        a:`${botPE[0].n} spends ${f$(botPE[0].pe)} per enrollee, ${((1-botPE[0].pe/natlPE)*100).toFixed(0)}% below the national average of ${f$(natlPE)}.`,
        data:botPE.map(s=>({n:s.n.substring(0,12),v:s.pe})), color:cB, unit:"$",
        action:()=>{setMM("pe");}
      });
    }
    // Home care
    const hcbs = codes.filter(h=>/HCBS|Waiver|T101|T202|S513/.test(h.c+h.cat));
    if (hcbs.length > 0) {
      const hcRates = SL.map(k=>{
        const rates = hcbs.map(h=>safe(h.r?.[k])).filter(v=>v>0);
        return {k,n:states[k]?.name||k,avg:rates.length>0?rates.reduce((a,b)=>a+b,0)/rates.length:0};
      }).filter(s=>s.avg>0).sort((a,b)=>a.avg-b.avg);
      const lo = hcRates.slice(0,5);
      if (lo.length > 2) {
        ins.push({
          id:"homecare", q:"Where is home care paid the least?",
          a:`${lo[0].n} averages ${f$(lo[0].avg)} across HCBS codes — the lowest in the dataset. ${lo[1].n} (${f$(lo[1].avg)}) and ${lo[2].n} (${f$(lo[2].avg)}) are close behind.`,
          data:lo.map(s=>({n:s.n.substring(0,12),v:s.avg})), color:NEG, unit:"$",
          action:()=>{setQ("home care");setBC("All");setTab("rate");}
        });
      }
    }
    // Behavioral health access
    const bhDens = SL.map(k=>{const st=g(k);return {k,n:st.name,d:st.enroll>0?safe(st.bh)/(st.enroll/1e3):0};}).filter(s=>s.d>0).sort((a,b)=>a.d-b.d);
    const bhLow = bhDens.slice(0,5);
    if (bhLow.length > 2) {
      ins.push({
        id:"bh_access", q:"Where is behavioral health access thinnest?",
        a:`${bhLow[0].n} has ${bhLow[0].d.toFixed(1)} behavioral health providers per 1,000 enrollees. ${bhLow[1].n} (${bhLow[1].d.toFixed(1)}) and ${bhLow[2].n} (${bhLow[2].d.toFixed(1)}) round out the bottom three.`,
        data:bhLow.map(s=>({n:s.n.substring(0,12),v:+s.d.toFixed(1)})), color:NEG, unit:"",
        action:()=>{setAC("bh");setMM("ac");}
      });
    }
    // Dental density
    const dnDens = SL.map(k=>{const st=g(k);return {k,n:st.name,d:st.enroll>0?safe(st.dn)/(st.enroll/1e3):0};}).filter(s=>s.d>0).sort((a,b)=>a.d-b.d);
    const dnLow = dnDens.slice(0,5);
    if (dnLow.length > 2) {
      ins.push({
        id:"dental", q:"Where are dental providers scarcest?",
        a:`With ${dnLow[0].d.toFixed(1)} dental providers per 1,000 enrollees, ${dnLow[0].n} has the thinnest Medicaid dental network. ${dnLow[1].n} and ${dnLow[2].n} also lag behind.`,
        data:dnLow.map(s=>({n:s.n.substring(0,12),v:+s.d.toFixed(1)})), color:cO, unit:"",
        action:()=>{setAC("dn");setMM("ac");}
      });
    }
    // Spending growth
    if (trends.length >= 3) {
      const first = trends[0], last = trends[trends.length-1];
      if (first.s > 0 && first.pe > 0) {
        const growth = ((last.s/first.s-1)*100).toFixed(0);
        const peGrowth = ((last.pe/first.pe-1)*100).toFixed(0);
        ins.push({
          id:"growth", q:"How fast is Medicaid spending growing?",
          a:`Total spending grew ${growth}% from ${first.y} to ${last.y} ($${first.s.toFixed(0)}B to $${last.s.toFixed(0)}B). Per-enrollee costs rose ${peGrowth}%, outpacing enrollment changes.`,
          data:trends.map(t=>({n:String(t.y),v:+t.s.toFixed(0)})), color:cB, unit:"$B",
          action:()=>{setMM("s");}
        });
      }
    }
    // Concentration
    const concCodes = codes.filter(h=>h.cn&&h.cn.gi>0).sort((a,b)=>(b.cn?.gi ?? 0)-(a.cn?.gi ?? 0)).slice(0,5);
    if (concCodes.length > 2) {
      ins.push({
        id:"conc", q:"Which services are dominated by a few providers?",
        a:`${concCodes[0].d} (${concCodes[0].c}) has a Gini coefficient of ${(concCodes[0].cn?.gi ?? 0).toFixed(2)}. The top 1% of providers account for ${concCodes[0].cn?.t1 ?? 0}% of spending on this code.`,
        data:concCodes.map(s=>({n:s.c,v:+(s.cn?.gi ?? 0).toFixed(2)})), color:cO, unit:"gini",
        action:()=>{setDC(concCodes[0].c);setTab("code");}
      });
    }
    // Maternity variation
    const mat = codes.find(h=>/5940/.test(h.c)&&h.r);
    if (mat) {
      const mRates = Object.entries(mat.r).map(([k,v])=>({k,n:states[k]?.name||k,v:safe(v)})).filter(s=>s.v>0).sort((a,b)=>b.v-a.v);
      if (mRates.length > 4) {
        const spread = mRates[0].v - mRates[mRates.length-1].v;
        ins.push({
          id:"maternity", q:"How much does maternity care cost vary?",
          a:`Vaginal delivery (${mat.c}) ranges from ${f$(mRates[mRates.length-1].v)} in ${mRates[mRates.length-1].n} to ${f$(mRates[0].v)} in ${mRates[0].n} for a ${f$(spread)} spread. ${mRates[0].n} pays ${((mRates[0].v/mRates[mRates.length-1].v-1)*100).toFixed(0)}% more than the lowest state.`,
          data:mRates.slice(0,5).map(s=>({n:s.n.substring(0,12),v:s.v})), color:cG, unit:"$",
          action:()=>{setDC(mat.c);setTab("code");}
        });
      }
    }
    // Case mix: price vs utilization
    const cmStates = SL.map(k=>({k,n:(states[k]?.name||k),pi:safe(states[k]?.pi,1),mi:safe(states[k]?.mi,1)})).filter(s=>s.pi>0.5&&s.pi<2&&s.mi>0.5&&s.mi<2);
    if (cmStates.length > 10) {
      const highPrice = [...cmStates].sort((a,b)=>b.pi-a.pi).slice(0,3);
      const heavyMix = [...cmStates].sort((a,b)=>b.mi-a.mi).slice(0,3);
      ins.push({
        id:"casemix", q:"Is high spending driven by prices or service mix?",
        a:`${highPrice[0].n} has the highest price index (${highPrice[0].pi.toFixed(3)}), paying ${((highPrice[0].pi-1)*100).toFixed(0)}% above national average rates for the same services. ${heavyMix[0].n} has the heaviest case mix (${heavyMix[0].mi.toFixed(3)}), using costlier services regardless of what it pays per unit.`,
        data:highPrice.map(s=>({n:s.n.substring(0,12),v:+((s.pi-1)*100).toFixed(1)})), color:cO, unit:"%",
        action:()=>{setMM("pe");}
      });
    }
    return ins;
  }, [SL, states, codes, trends, natlPE, g]);


  const Sel = ({ value, onChange, label, optional }: { value: string; onChange: (v: string) => void; label: string; optional?: boolean }) => (
    <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
      {label && <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>{label}</span>}
      <select value={value} onChange={e => onChange(e.currentTarget.value)}
        style={{ background: S, border: `1px solid ${B}`, padding: "5px 10px", borderRadius: 6, fontSize: 11, color: value ? A : AL }}>
        {optional && <option value="">— None —</option>}
        {SL.map(k => <option key={k} value={k}>{states[k]?.name || k}</option>)}
      </select>
    </div>
  );

  const bench = useMemo(() => {
    let base = codes.filter(h => {
      if (bCat !== "All" && h.cat !== bCat) return false;
      return h.r && h.r[s1] !== undefined;
    });
    if (q) base = expandSearch(q, base);
    const totalNatlSpend = SL.reduce((a,k)=>a+safe(states[k]?.spend),0);
    const stShare = totalNatlSpend > 0 ? safe(states[s1]?.spend) / totalNatlSpend : 0;
    const has2 = s2 && s2 !== s1;
    const has3 = s3 && s3 !== s1 && s3 !== s2;
    const adj1 = mixAdj ? getAdj(s1) : 1;
    let list = base.map(h => {
      const r1 = safe(h.r[s1]);
      const r2 = has2 ? safe(h.r[s2]) : 0;
      const r3 = has3 ? safe(h.r[s3]) : 0;
      const naRef = safe(h.na) * adj1; // adjusted national avg when mixAdj on
      const estStateClaims = safe(h.nc) * stShare;
      const mc = getMcRate(h.c);
      const fs1 = getFsRate(s1, h.c);
      return { ...h, r1, r2, r3, mc, fs1, naRef, g1: pD(r1, naRef), g2: has2 && r2>0 ? pD(r2, naRef) : null, g3: has3 && r3>0 ? pD(r3, naRef) : null, fi: (naRef - r1) * estStateClaims };
    });
    type BenchItem = (typeof list)[number];
    const sf: Record<string, (a: BenchItem, b: BenchItem) => number> = { gap:(a,b)=>Math.abs(b.g1)-Math.abs(a.g1), high:(a,b)=>b.r1-a.r1, fiscal:(a,b)=>Math.abs(b.fi)-Math.abs(a.fi) };
    list.sort(sf[bSort] || sf.fiscal);
    return list.slice(0, 200);
  }, [codes, s1, s2, s3, bCat, bSort, q, states, getMcRate, getFsRate, mixAdj, getAdj]);

  // Category-level summary for Rate Engine overview
  const catSummary = useMemo(() => {
    const has2 = s2 && s2 !== s1;
    const has3 = s3 && s3 !== s1 && s3 !== s2;
    const cats: Record<string, CatAccumulator> = {};
    codes.forEach(h => {
      if (!h.r || h.r[s1] === undefined) return;
      const cat = h.cat || "Other";
      if (!cats[cat]) cats[cat] = { cat, s1W: 0, s2W: 0, s3W: 0, naW: 0, w: 0, n: 0, s2n: 0, s3n: 0 };
      const w = safe(h.nc);
      cats[cat].s1W += safe(h.r[s1]) * w;
      cats[cat].naW += safe(h.na) * w;
      if (has2 && h.r[s2] !== undefined) { cats[cat].s2W += safe(h.r[s2]) * w; cats[cat].s2n += 1; }
      if (has3 && h.r[s3] !== undefined) { cats[cat].s3W += safe(h.r[s3]) * w; cats[cat].s3n += 1; }
      cats[cat].w += w;
      cats[cat].n += 1;
    });
    const adj1 = getAdj(s1), adj2 = has2 ? getAdj(s2) : 1, adj3 = has3 ? getAdj(s3) : 1;
    return Object.values(cats)
      .filter(c => c.w > 0)
      .map(c => {
        const na = c.naW / c.w;
        return {
          cat: c.cat,
          s1: c.s1W / c.w, s2: c.s2W > 0 ? c.s2W / c.w : null, s3: c.s3W > 0 ? c.s3W / c.w : null,
          na, naAdj: na * adj1,
          g1: pD(c.s1W / c.w, na), g1a: pD(c.s1W / c.w, na * adj1),
          g2: c.s2W > 0 ? pD(c.s2W / c.w, na) : null, g2a: c.s2W > 0 ? pD(c.s2W / c.w, na * adj2) : null,
          g3: c.s3W > 0 ? pD(c.s3W / c.w, na) : null, g3a: c.s3W > 0 ? pD(c.s3W / c.w, na * adj3) : null,
          codes: c.n, claims: c.w,
        };
      })
      .sort((a, b) => b.claims - a.claims);
  }, [codes, s1, s2, s3, getAdj]);

  // Overall weighted averages
  const rateOverview = useMemo(() => {
    if (!catSummary.length) return null;
    const totalW = catSummary.reduce((a, c) => a + c.claims, 0);
    const s1All = catSummary.reduce((a, c) => a + c.s1 * c.claims, 0) / totalW;
    const naAll = catSummary.reduce((a, c) => a + c.na * c.claims, 0) / totalW;
    const adj1 = getAdj(s1);
    const naAdjAll = naAll * adj1;
    const raData = riskAdj?.states?.[s1];
    return { s1All, naAll, naAdjAll, adj1, totalW, adjPe: raData?.adjusted_pe };
  }, [catSummary, s1, getAdj, riskAdj]);

  const dC = useMemo(() => codes.find(h => h.c === dc) || null, [codes, dc]);
  const dCS = useMemo(() => {
    if (!dC?.r) return [];
    return Object.entries(dC.r).map(([ab, r]) => ({ ab, r: safe(r), n: states[ab]?.name || ab, gp: pD(safe(r), dC.na) })).sort((a, b) => b.r - a.r);
  }, [dC, states]);

  interface RegionRecord { zip3?: string; region_name?: string; total_paid?: number; n_providers?: number; [key: string]: unknown }
  const stateRegions = useMemo((): RegionRecord[] | null => {
    const summary = (regions as Record<string, Record<string, RegionRecord[]>> | null)?.summary;
    const r = summary?.[s1];
    return Array.isArray(r) ? [...r].sort((a, b) => safe(a.total_paid) - safe(b.total_paid)).reverse() : null;
  }, [regions, s1]);

  const ranking = useMemo(() => SL.map(k => ({ k, ...g(k) })).sort((a, b) => curM.fn(b) - curM.fn(a)), [SL, curM, states]);
  const rankMax = ranking.length > 0 ? Math.max(curM.fn(ranking[0]), 1) : 1;

  if (loading) return (
    <div style={{ display:"flex",justifyContent:"center",alignItems:"center",minHeight:400,fontFamily:"Helvetica Neue,Arial,sans-serif" }}>
      <style>{`@keyframes ember{0%,100%{color:#0A2540}50%{color:#C4590A}}`}</style>
      <div style={{ textAlign:"center" }}><div style={{ fontSize:16,fontWeight:600,animation:"ember 2.4s ease-in-out infinite" }}>Loading Spending Data...</div><div style={{ fontSize:11,color:AL,marginTop:4 }}>Preparing explorer</div></div>
    </div>
  );

  const TABS = [{k:"dash",l:"Dashboard"},{k:"rate",l:"Rate Engine"},{k:"code",l:"Code Profile"},{k:"sim",l:"Simulator"},{k:"provider",l:"Providers"},{k:"about",l:"About"}];

  return (
    <div style={{ maxWidth:960,margin:"0 auto",padding:"10px 16px 40px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>

      {/* Tool Header */}
      <div style={{ paddingBottom:8,borderBottom:`1px solid ${B}`,marginBottom:12,display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:6 }}>
        <div style={{ display:"flex",alignItems:"center",gap:6 }}>
          {!isLive && <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(184,134,11,0.12)",color:WARN,fontWeight:600 }}>PROTOTYPE</span>}
          {isLive && <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(14,98,69,0.1)",color:POS,fontWeight:600 }}>LIVE DATA</span>}
          {isLive && Array.isArray(meta?.years) && <span style={{ fontSize:9,color:AL,fontFamily:FM }}>{(meta.years as number[])[0]}–{(meta.years as number[])[(meta.years as number[]).length-1]}</span>}
        </div>
        <div style={{ display:"flex",gap:1,flexWrap:"wrap" }}>
          {TABS.map(t => <button key={t.k} onClick={()=>setTab(t.k)} style={{ padding:"4px 8px",fontSize:10,fontWeight:tab===t.k?700:400,color:tab===t.k?cB:AL,background:tab===t.k?"rgba(46,107,74,0.05)":"transparent",border:"none",borderRadius:6,cursor:"pointer",borderBottom:tab===t.k?`2px solid ${cB}`:"2px solid transparent",whiteSpace:"nowrap" }}>{t.l}</button>)}
        </div>
      </div>

      {/* DASHBOARD */}
      {tab==="dash" && <div style={{ display:"grid",gap:10 }}>
        <div style={{ display:"flex",gap:8,alignItems:"flex-start",justifyContent:"space-between",flexWrap:"wrap" }}>
          <TabGuide title="Dashboard" desc="National overview of Medicaid spending. Pick a state and compare metric to see how it ranks. The hex map colors states by the selected metric; click any state to select it." tips="Try switching to Access mode and picking a provider type to see provider-per-enrollee density by state."/>
          <ExportBtn label="Export States" onClick={()=>{
            const hdr=["State","Spend","Enrollment","Per Enrollee","FMAP","MC%","Providers","E&M Provs","HCBS Provs","BH Provs","Dental Provs"];
            const rows=SL.map(k=>{const s=states[k];return [s?.name||k,safe(s?.spend),safe(s?.enroll),safe(s?.pe),s?.fmap,s?.mc,safe(s?.provs),safe(s?.em),safe(s?.hcbs),safe(s?.bh),safe(s?.dn)];});
            downloadCSV(`medicaid_dashboard_${s1}.csv`,hdr,rows);
          }}/>
        </div>
        {/* KPI Strip */}
        <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(80px,1fr))",gap:6 }}>
          <Card><Met l="States" v={SL.length}/></Card>
          <Card><Met l="Total Spend" v={f$(SL.reduce((a,k)=>a+safe(states[k]?.spend),0))}/></Card>
          <Card><Met l="Enrollment" v={fN(SL.reduce((a,k)=>a+safe(states[k]?.enroll),0))}/></Card>
          <Card><Met l="Per Enrollee" v={f$(natlPE)}/></Card>
          <Card><Met l="HCPCS Codes" v={codes.length.toLocaleString()}/></Card>
        </div>
        {/* Controls */}
        <div style={{ display:"flex",gap:10,alignItems:"flex-end",flexWrap:"wrap" }}>
          <Sel value={s1} onChange={setS1} label="State"/><Sel value={s2} onChange={setS2} label="Compare" optional/>
          <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
            <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Metric</span>
            <div style={{ display:"flex",gap:3 }}>{Object.entries(mms).map(([k,v])=><Pill key={k} on={mm===k} onClick={()=>setMM(k)}>{v.l}</Pill>)}</div>
          </div>
          {mm==="pe" && hasAdj && <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
            <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Adjust</span>
            <div style={{ display:"flex",gap:3 }}>
              <Pill on={peMode==="raw"} onClick={()=>setPEM("raw")}>Raw</Pill>
              <Pill on={peMode==="adj"} onClick={()=>setPEM("adj")}>Mix-Adj</Pill>
            </div>
          </div>}
          {/* Reference data indicators */}
          {(mcRates||feeScheds||riskAdj) && <div style={{ display:"flex",gap:4,alignItems:"center",marginLeft:"auto" }}>
            {mcRates && <span style={{ fontSize:8,color:POS,fontFamily:FM,background:"rgba(46,107,74,0.08)",padding:"2px 5px",borderRadius:3 }}>Medicare ✓</span>}
            {feeScheds && <span style={{ fontSize:8,color:POS,fontFamily:FM,background:"rgba(46,107,74,0.08)",padding:"2px 5px",borderRadius:3 }}>Fee Sched ✓</span>}
            {riskAdj && <span style={{ fontSize:8,color:POS,fontFamily:FM,background:"rgba(46,107,74,0.08)",padding:"2px 5px",borderRadius:3 }}>Risk Adj ✓</span>}
          </div>}
        </div>
        {/* Access sub-pills: choose provider type when in Access mode */}
        {mm==="ac" && <div style={{ display:"flex",gap:3,alignItems:"center" }}>
          <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5,marginRight:2 }}>Provider Type</span>
          {ACC.map(sc=><Pill key={sc.k} on={ac===sc.k} onClick={()=>setAC(sc.k)}>{sc.l}</Pill>)}
        </div>}
        {/* Map + State cards */}
        <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:10 }}>
          <Card><div style={{ padding:"8px 14px" }}><HexMap states={states} fn={curM.fn} fmt={curM.fc||curM.f} onSel={setS1} sel={s1}/></div></Card>
          <div style={{ display:"grid",gap:8 }}>
            <Card accent={d1Dev>0?NEG:POS}>
              <div style={{ padding:"10px 16px 6px" }}><div style={{ display:"flex",justifyContent:"space-between",alignItems:"baseline" }}><span style={{ fontSize:18,fontWeight:300 }}>{d1.name||s1}</span><span style={{ fontSize:11,fontFamily:FM,fontWeight:600,color:d1Dev>0?NEG:POS }}>{d1Dev>0?"+":""}{d1Dev.toFixed(1)}% <span style={{fontWeight:400,color:AL,fontSize:9}}>vs natl avg</span></span></div></div>
              <div style={{ display:"grid",gridTemplateColumns:"repeat(3,1fr)",padding:"0 6px 8px" }}>
                <Met l="Spend" v={f$(d1.spend)}/><Met l="Enroll" v={fN(d1.enroll)}/><Met l={peMode==="adj"?"Per Cap (adj)":"Per Cap"} v={`$${safe(d1PE).toLocaleString(undefined,{maximumFractionDigits:0})}`}/>
                <Met l="FMAP" v={`${d1.fmap}%`}/><Met l="MC%" v={`${d1.mc}%`}/><Met l="Codes" v={codes.filter(h=>h.r&&h.r[s1]!==undefined).length.toLocaleString()}/>
              </div>
              {mm==="ac" && (() => { const sc=ACC.find(x=>x.k===ac)||ACC[0]; const cnt=sc.fn(d1); const per1k=safe(d1.enroll)>0?cnt/(safe(d1.enroll)/1e3):0; const natlCnt=SL.reduce((a,k)=>a+sc.fn(g(k)),0); const natlEnr=SL.reduce((a,k)=>a+safe(states[k]?.enroll),0); const natlPer1k=natlEnr>0?natlCnt/(natlEnr/1e3):0; const dev=natlPer1k>0?pD(per1k,natlPer1k):0; return <div style={{ padding:"0 10px 8px",borderTop:`1px solid ${B}`,marginTop:2,paddingTop:6 }}>
                <div style={{ fontSize:9,fontFamily:FM,display:"flex",gap:12,alignItems:"center" }}>
                  <span style={{ fontWeight:600 }}>{sc.l} Providers</span>
                  <span>{fN(cnt)} total</span>
                  <span>{per1k.toFixed(1)}/1K enrollees</span>
                  <span style={{ color:dev<0?NEG:POS,fontWeight:600 }}>{dev>0?"+":""}{dev.toFixed(1)}% vs natl</span>
                </div>
              </div>; })()}
            </Card>
            {s2 && s2!==s1 && (() => { const d2=g(s2); const d2PE=(peMode==="adj"&&hasAdj)?(()=>{const f=getAdj(s2);return f>0?safe(d2.pe)/f:safe(d2.pe);})():safe(d2.pe); const d2Dev=pD(d2PE,natlPEAdj); const d2vs1=d1PE>0?((d2PE-d1PE)/d1PE*100):0; return <Card accent={d2Dev>0?NEG:POS}>
              <div style={{ padding:"10px 16px 6px" }}><div style={{ display:"flex",justifyContent:"space-between",alignItems:"baseline" }}><span style={{ fontSize:18,fontWeight:300 }}>{d2.name||s2}</span><span style={{ fontSize:11,fontFamily:FM,fontWeight:600,color:d2Dev>0?NEG:POS }}>{d2Dev>0?"+":""}{d2Dev.toFixed(1)}% <span style={{fontWeight:400,color:AL,fontSize:9}}>vs natl avg</span></span></div></div>
              <div style={{ display:"grid",gridTemplateColumns:"repeat(3,1fr)",padding:"0 6px 8px" }}>
                <Met l="Spend" v={f$(d2.spend)}/><Met l="Enroll" v={fN(d2.enroll)}/><Met l={peMode==="adj"?"Per Cap (adj)":"Per Cap"} v={`$${safe(d2PE).toLocaleString(undefined,{maximumFractionDigits:0})}`}/>
                <Met l="FMAP" v={`${d2.fmap}%`}/><Met l="MC%" v={`${d2.mc}%`}/><Met l="Codes" v={codes.filter(h=>h.r&&h.r[s2]!==undefined).length.toLocaleString()}/>
              </div>
              {mm==="ac" && (() => { const sc=ACC.find(x=>x.k===ac)||ACC[0]; const cnt=sc.fn(d2); const per1k=safe(d2.enroll)>0?cnt/(safe(d2.enroll)/1e3):0; const cnt1=sc.fn(d1); const per1k1=safe(d1.enroll)>0?cnt1/(safe(d1.enroll)/1e3):0; const dev=per1k1>0?pD(per1k,per1k1):0; return <div style={{ padding:"0 10px 4px",borderTop:`1px solid ${B}`,marginTop:2,paddingTop:6 }}>
                <div style={{ fontSize:9,fontFamily:FM,display:"flex",gap:12,alignItems:"center" }}>
                  <span style={{ fontWeight:600 }}>{sc.l} Providers</span>
                  <span>{fN(cnt)} total</span>
                  <span>{per1k.toFixed(1)}/1K enrollees</span>
                  <span style={{ color:dev<0?NEG:POS,fontWeight:600 }}>{dev>0?"+":""}{dev.toFixed(1)}% vs {d1.name||s1}</span>
                </div>
              </div>; })()}
              <div style={{ padding:"2px 10px 8px" }}>
                <div style={{ fontSize:9,color:AL }}>vs {d1.name||s1}: per cap {d2vs1>=0?"+":""}{d2vs1.toFixed(1)}% · spend {d1.spend>0?((d2.spend-d1.spend)/d1.spend*100>=0?"+":"")+""+((d2.spend-d1.spend)/d1.spend*100).toFixed(1)+"%":"—"}</div>
              </div>
            </Card>; })()}
            <Card x><CH t="Ranking" b={curM.l}/><div style={{ padding:"0 14px 6px",maxHeight:180,overflowY:"auto" }}>
              {ranking.map((st,i)=>(
                <div key={st.k} style={{ display:"flex",alignItems:"center",gap:5,fontSize:10,padding:"2px 0",cursor:"pointer",background:s2&&st.k===s2&&s2!==s1?"rgba(184,134,11,0.06)":"transparent" }} onClick={()=>setS1(st.k)}>
                  <span style={{ width:16,fontFamily:FM,color:AL,fontSize:8,textAlign:"right" }}>{i+1}</span>
                  <span style={{ width:80,fontWeight:st.k===s1||(s2&&st.k===s2)?600:400,color:s2&&st.k===s2&&s2!==s1?WARN:A }}>{st.name}</span>
                  <div style={{ flex:1,height:6,background:S,borderRadius:2 }}><div style={{ width:`${rankMax>0?(curM.fn(st)/rankMax)*100:0}%`,height:"100%",background:st.k===s1?cB:(s2&&st.k===s2)?WARN:"rgba(46,107,74,0.27)",borderRadius:2 }}/></div>
                  <span style={{ width:50,textAlign:"right",fontFamily:FM,fontSize:9 }}>{curM.f(curM.fn(st))}</span>
                </div>
              ))}
            </div></Card>
          </div>
        </div>
        {/* Regions (if pipeline data available) */}
        {stateRegions && stateRegions.length>0 && <Card x><CH t={`Regions in ${states[s1]?.name||s1}`} b={`${stateRegions.length} ZIP3 areas`}/><div style={{ padding:"0 14px 8px",maxHeight:200,overflowY:"auto" }}>
          {stateRegions.slice(0,30).map((r,i)=>{ const mx=safe(stateRegions[0]?.total_paid,1); return (
            <div key={i} style={{ display:"flex",alignItems:"center",gap:5,fontSize:10,padding:"2px 0" }}>
              <span style={{ width:28,fontFamily:FM,color:AL,fontSize:8 }}>{r.zip3}</span>
              <span style={{ width:100,color:A,fontSize:9 }}>{r.region_name}</span>
              <div style={{ flex:1,height:6,background:S,borderRadius:2 }}><div style={{ width:`${(safe(r.total_paid)/mx)*100}%`,height:"100%",background:cT,borderRadius:2,opacity:0.6 }}/></div>
              <span style={{ width:55,textAlign:"right",fontFamily:FM,fontSize:9 }}>{f$(safe(r.total_paid))}</span>
              <span style={{ width:40,textAlign:"right",fontFamily:FM,fontSize:8,color:AL }}>{fN(safe(r.n_providers))}p</span>
            </div>); })}
        </div></Card>}
        {/* Case mix + Trend */}
        <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:10 }}>
          {(() => {
            const cmData = SL.map(k=>({k,n:(states[k]?.name||k).substring(0,12),pi:safe(states[k]?.pi,1),mi:safe(states[k]?.mi,1),pe:safe(states[k]?.pe)})).filter(s=>s.pi>0.5&&s.pi<2&&s.mi>0.5&&s.mi<2);
            return cmData.length > 5 ? <Card x><CH t="Case Mix: Price vs Utilization" b="1.0 = national avg"/><div style={{ padding:"0 14px 8px" }}>
              <ResponsiveContainer width="100%" height={220}>
                <ScatterChart margin={{top:10,right:10,bottom:5,left:5}}>
                  <CartesianGrid strokeDasharray="3 3" stroke={B}/>
                  <XAxis type="number" dataKey="pi" name="Price" domain={["auto","auto"]} tick={{fill:AL,fontSize:9,fontFamily:FM}} axisLine={false} tickLine={false} label={{value:"Price Index →",position:"bottom",fontSize:9,fill:AL,offset:-2}}/>
                  <YAxis type="number" dataKey="mi" name="Mix" domain={["auto","auto"]} tick={{fill:AL,fontSize:9,fontFamily:FM}} axisLine={false} tickLine={false} label={{value:"Mix Index →",angle:-90,position:"left",fontSize:9,fill:AL}}/>
                  <ReferenceLine x={1} stroke={B} strokeDasharray="4 4"/>
                  <ReferenceLine y={1} stroke={B} strokeDasharray="4 4"/>
                  <Tooltip content={<SafeTip render={(_d)=>{ const d = _d as {k:string;pi:number;mi:number;pe:number}; return d.k?(<div><div style={{fontWeight:600}}>{states[d.k]?.name||d.k}</div><div>Price: {d.pi?.toFixed(3)} ({d.pi>1?"+":""}{ ((d.pi-1)*100).toFixed(1)}%)</div><div>Mix: {d.mi?.toFixed(3)} ({d.mi>1?"+":""}{((d.mi-1)*100).toFixed(1)}%)</div><div style={{color:AL,fontSize:9}}>Per enrollee: {f$(d.pe)}</div></div>):null; }}/>}/>
                  <Scatter data={cmData} fill={cB} stroke={cB}>
                    {cmData.map(s=><Cell key={s.k} fill={s.k===s1?cO:(s2&&s.k===s2)?WARN:cB} stroke={s.k===s1?cO:(s2&&s.k===s2)?WARN:cB} r={s.k===s1||(s2&&s.k===s2)?6:4} opacity={s.k===s1||(s2&&s.k===s2)?1:0.6}/>)}
                  </Scatter>
                </ScatterChart>
              </ResponsiveContainer>
              <div style={{ fontSize:9,color:AL,lineHeight:1.5,padding:"4px 0" }}>
                <b>Top-right:</b> pays more AND uses costlier services · <b>Bottom-left:</b> pays less AND lighter mix
              </div>
            </div></Card> : null;
          })()}
          {trends.length > 2 && <Card x><CH t="National Trend (Indexed)" b={`${trends[0]?.y||2018}=100`}/><div style={{ padding:"0 14px 8px" }}>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart margin={{right:20}} data={trends.map(d=>({...d,si:+(safe(d.s)/safe(trends[0]?.s,1)*100).toFixed(1),ei:+(safe(d.e)/safe(trends[0]?.e,1)*100).toFixed(1),pi:+(safe(d.pe)/safe(trends[0]?.pe,1)*100).toFixed(1)}))}>
                <CartesianGrid strokeDasharray="3 3" stroke={B} vertical={false}/>
                <XAxis dataKey="y" tick={{fill:AL,fontSize:9}} axisLine={false} tickLine={false} interval={0}/>
                <YAxis tick={{fill:AL,fontSize:9}} axisLine={false} tickLine={false} domain={[80,'auto']}/>
                <Tooltip content={<SafeTip render={(_d)=>{ const d=_d as {y:number;si:number;ei:number;pi:number}; return (<div><div style={{fontWeight:600}}>{d.y}</div><div style={{color:cB}}>Spend: {d.si}</div><div style={{color:cG}}>Enroll: {d.ei}</div><div style={{color:cO}}>Per cap: {d.pi}</div></div>); }}/>}/>
                <Line type="monotone" dataKey="si" stroke={cB} strokeWidth={2.5} dot={{r:2}} name="Spend"/>
                <Line type="monotone" dataKey="ei" stroke={cG} strokeWidth={2.5} dot={{r:2}} name="Enroll"/>
                <Line type="monotone" dataKey="pi" stroke={cO} strokeWidth={2.5} dot={{r:2}} strokeDasharray="6 3" name="Per Cap"/>
                <Legend wrapperStyle={{fontSize:9}}/>
              </LineChart>
            </ResponsiveContainer>
          </div></Card>}
        </div>
        {/* Insights */}
        {insights.length > 0 && <div>
          <div style={{ fontSize:10,fontWeight:600,color:AL,textTransform:"uppercase",letterSpacing:0.5,marginBottom:6 }}>Explore the data</div>
          <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(340px,1fr))",gap:6 }}>
            {(showAllIns ? insights : insights.slice(0,4)).map(ins => {
              const open = insightOpen === ins.id;
              return <div key={ins.id} style={{ background:WH,borderRadius:10,boxShadow:SH,overflow:"hidden",cursor:"pointer",borderLeft:`3px solid ${ins.color}`,transition:"all 0.15s ease" }} onClick={()=>setIO(open?null:ins.id)}>
                <div style={{ padding:"8px 12px 6px",display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:8 }}>
                  <span style={{ fontSize:11,fontWeight:500,color:A,lineHeight:1.4 }}>{ins.q}</span>
                  <span style={{ fontSize:10,color:AL,flexShrink:0,marginTop:1 }}>{open?"−":"+"}</span>
                </div>
                {open && <div style={{ padding:"0 12px 10px" }}>
                  <div style={{ fontSize:11,color:AL,lineHeight:1.6,marginBottom:8 }}>{ins.a}</div>
                  {ins.data && <div style={{ marginBottom:6 }}>
                    <ResponsiveContainer width="100%" height={ins.data.length > 5 ? 100 : 80}>
                      <BarChart data={ins.data} layout="vertical" margin={{left:0,right:8}}>
                        <XAxis type="number" tick={{fill:AL,fontSize:8,fontFamily:FM}} axisLine={false} tickLine={false} tickFormatter={v=>ins.unit==="$"?f$(v):ins.unit==="$B"?`$${v}B`:String(v)}/>
                        <YAxis type="category" dataKey="n" tick={{fill:A,fontSize:9,fontFamily:FM}} axisLine={false} tickLine={false} width={70}/>
                        <Bar dataKey="v" barSize={10} radius={[0,3,3,0]} fill={ins.color} opacity={0.7}/>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>}
                  <button onClick={e=>{e.stopPropagation();ins.action();}} style={{ fontSize:10,color:ins.color,background:"none",border:`1px solid ${ins.color}`,borderRadius:5,padding:"3px 10px",cursor:"pointer",fontWeight:600 }}>Explore this →</button>
                </div>}
              </div>;
            })}
          </div>
          {insights.length > 4 && <button onClick={()=>setSAI(!showAllIns)} style={{ marginTop:4,fontSize:10,color:AL,background:"none",border:"none",cursor:"pointer",padding:"4px 0" }}>{showAllIns?`Show fewer ↑`:`${insights.length-4} more questions ↓`}</button>}
        </div>}
      </div>}

      {/* RATE ENGINE */}
      {tab==="rate" && (() => {
        const has2 = s2 && s2 !== s1;
        const has3 = s3 && s3 !== s1 && s3 !== s2;
        const compLabel = [states[s1]?.name||s1, has2&&(states[s2]?.name||s2), has3&&(states[s3]?.name||s3)].filter(Boolean).join(" · ");
        return <div style={{ display:"grid",gap:10 }}>
        <div style={{ display:"flex",gap:8,alignItems:"flex-start",justifyContent:"space-between",flexWrap:"wrap" }}>
          <TabGuide title="Rate Engine" desc="Compare what Medicaid pays per claim across states, benchmarked against the national average, Medicare, and fee schedules. The category table shows weighted averages; the code table below lets you drill into individual HCPCS codes." tips="Select up to 3 states. Use Raw/Mix-Adj to account for differences in enrollment demographics. Click a category row to filter the code list."/>
          <ExportBtn onClick={()=>{
            const hdr=["Code","Description","Category",states[s1]?.name||s1,has2?(states[s2]?.name||s2):"",has3?(states[s3]?.name||s3):"","National","Medicare","Fee Sched","Gap %","Est Impact"].filter(Boolean);
            const rows=bench.map(d=>{const r=[d.c,d.d,d.cat,d.r1.toFixed(2)];if(has2)r.push(d.r2>0?d.r2.toFixed(2):"");if(has3)r.push(d.r3>0?d.r3.toFixed(2):"");r.push(d.na.toFixed(2),d.mc>0?d.mc.toFixed(2):"",d.fs1>0?d.fs1.toFixed(2):"",d.g1.toFixed(1),d.fi.toFixed(0));return r;});
            downloadCSV(`medicaid_rates_${s1}.csv`,hdr,rows);
          }}/>
        </div>
        <div style={{ display:"flex",gap:8,alignItems:"flex-end",flexWrap:"wrap" }}>
          <Sel value={s1} onChange={setS1} label="Primary State"/>
          <Sel value={s2} onChange={setS2} label="Compare 1" optional/>
          <Sel value={s3} onChange={setS3} label="Compare 2" optional/>
          <div style={{ display:"flex",flexDirection:"column",gap:2 }}>
            <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Category</span>
            <select value={bCat} onChange={e=>setBC(e.currentTarget.value)} style={{ background:S,border:`1px solid ${B}`,padding:"6px 8px",borderRadius:6,fontSize:11 }}>
              <option value="All">All Categories ({codes.filter(h=>h.r&&h.r[s1]!==undefined).length})</option>
              {CATS.map(c=><option key={c} value={c}>{c} ({codes.filter(h=>h.r&&h.r[s1]!==undefined&&h.cat===c).length})</option>)}
            </select>
          </div>
          <div style={{ display:"flex",flexDirection:"column",gap:2,flex:1,maxWidth:200 }}>
            <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>Search Codes</span>
            <div style={{ position:"relative" }}>
              <input value={q} onChange={e=>setQ(e.currentTarget.value)} placeholder="Try 'dental', 'home care', 'office visit'..." style={{ width:"100%",background:S,border:`1px solid ${B}`,padding:"6px 10px 6px 24px",borderRadius:6,fontSize:11,outline:"none",boxSizing:"border-box" }}/>
              <span style={{ position:"absolute",left:7,top:"50%",transform:"translateY(-50%)",color:AL,fontSize:12 }}>&#x2315;</span>
            </div>
          </div>
        </div>

        {/* Category Overview */}
        {catSummary.length>0 && rateOverview && (() => {
          const has2 = s2 && s2 !== s1;
          const has3 = s3 && s3 !== s1 && s3 !== s2;
          const maxGap = catSummary.reduce((m,c) => {
            const vals = [Math.abs(mixAdj?c.g1a:c.g1)];
            if (c.g2!==null && c.g2a!==null) vals.push(Math.abs(mixAdj?(c.g2a ?? 0):(c.g2 ?? 0)));
            if (c.g3!==null && c.g3a!==null) vals.push(Math.abs(mixAdj?(c.g3a ?? 0):(c.g3 ?? 0)));
            return Math.max(m, ...vals);
          }, 20);
          const GapBar = ({gap, color}: {gap: number | null | undefined; color?: string}) => {
            if (gap === null || gap === undefined) return <span style={{color:B}}>—</span>;
            const w = Math.min(Math.abs(gap) / maxGap * 100, 100);
            return <div style={{ display:"flex",alignItems:"center",gap:4 }}>
              <div style={{ width:80,height:12,background:S,borderRadius:3,position:"relative",overflow:"hidden" }}>
                <div style={{ position:"absolute",left:"50%",top:0,bottom:0,width:1,background:B }}/>
                <div style={{ position:"absolute",top:1,bottom:1,borderRadius:2,
                  ...(gap>=0 ? {left:"50%",width:`${w/2}%`,background:color||POS} : {right:"50%",width:`${w/2}%`,background:NEG})
                }}/>
              </div>
              <span style={{ fontSize:9,fontFamily:FM,fontWeight:600,color:gap<-5?NEG:gap>5?(color||POS):AL,minWidth:38,textAlign:"right" }}>{gap>0?"+":""}{gap.toFixed(1)}%</span>
            </div>;
          };
          const stColors = [POS, "#C4590A", "#1565C0"];
          return <Card x><div style={{ padding:"14px 20px 10px" }}>
          <div style={{ display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10 }}>
            <div>
              <div style={{ fontSize:13,fontWeight:700,color:A }}>Rate Comparison by Category</div>
              <div style={{ fontSize:10,color:AL }}>Weighted avg rate per claim · gap vs national avg · {catSummary.reduce((a,c)=>a+c.codes,0).toLocaleString()} codes</div>
            </div>
            {riskAdj && <div style={{ display:"flex",gap:3,alignItems:"center" }}>
              <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5 }}>National</span>
              <Pill on={!mixAdj} onClick={()=>setMixAdj(false)}>Raw</Pill>
              <Pill on={mixAdj} onClick={()=>setMixAdj(true)}>Mix-Adj</Pill>
            </div>}
          </div>
          <div style={{ overflowX:"auto" }}>
            <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}>
              <thead><tr style={{ borderBottom:`2px solid ${B}` }}>
                <th style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>Category</th>
                <th style={{ textAlign:"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>Natl{mixAdj?" (adj)":""}</th>
                <th style={{ textAlign:"right",padding:"6px 4px",color:stColors[0],fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{states[s1]?.name||s1}</th>
                <th style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM,minWidth:120 }}>Gap</th>
                {has2&&<><th style={{ textAlign:"right",padding:"6px 4px",color:stColors[1],fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{states[s2]?.name||s2}</th>
                <th style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM,minWidth:120 }}>Gap</th></>}
                {has3&&<><th style={{ textAlign:"right",padding:"6px 4px",color:stColors[2],fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{states[s3]?.name||s3}</th>
                <th style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM,minWidth:120 }}>Gap</th></>}
                <th style={{ textAlign:"right",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>Codes</th>
              </tr></thead>
              <tbody>
                {catSummary.map(c => (
                  <tr key={c.cat} style={{ borderBottom:`1px solid ${B}`,cursor:"pointer" }} onClick={()=>setBC(c.cat)}>
                    <td style={{ padding:"5px 4px",fontWeight:600,fontSize:10 }}>{c.cat}</td>
                    <td style={{ padding:"5px 4px",fontFamily:FM,color:AL,textAlign:"right" }}>{f$(mixAdj?c.naAdj:c.na)}</td>
                    <td style={{ padding:"5px 4px",fontFamily:FM,fontWeight:600,textAlign:"right" }}>{f$(c.s1)}</td>
                    <td style={{ padding:"5px 4px" }}><GapBar gap={mixAdj?c.g1a:c.g1} color={stColors[0]}/></td>
                    {has2&&<><td style={{ padding:"5px 4px",fontFamily:FM,color:stColors[1],textAlign:"right" }}>{c.s2!==null?f$(c.s2):<span style={{color:B}}>—</span>}</td>
                    <td style={{ padding:"5px 4px" }}><GapBar gap={mixAdj?c.g2a:c.g2} color={stColors[1]}/></td></>}
                    {has3&&<><td style={{ padding:"5px 4px",fontFamily:FM,color:stColors[2],textAlign:"right" }}>{c.s3!==null?f$(c.s3):<span style={{color:B}}>—</span>}</td>
                    <td style={{ padding:"5px 4px" }}><GapBar gap={mixAdj?c.g3a:c.g3} color={stColors[2]}/></td></>}
                    <td style={{ padding:"5px 4px",fontFamily:FM,color:AL,textAlign:"right",fontSize:9 }}>{c.codes}</td>
                  </tr>
                ))}
                {/* Totals row */}
                <tr style={{ borderTop:`2px solid ${A}`,fontWeight:700 }}>
                  <td style={{ padding:"6px 4px",fontSize:10 }}>All Categories</td>
                  <td style={{ padding:"6px 4px",fontFamily:FM,color:AL,textAlign:"right" }}>{f$(mixAdj?rateOverview.naAdjAll:rateOverview.naAll)}</td>
                  <td style={{ padding:"6px 4px",fontFamily:FM,textAlign:"right" }}>{f$(rateOverview.s1All)}</td>
                  <td style={{ padding:"6px 4px" }}><GapBar gap={pD(rateOverview.s1All,mixAdj?rateOverview.naAdjAll:rateOverview.naAll)} color={stColors[0]}/></td>
                  {has2&&<><td style={{ padding:"6px 4px" }}/><td style={{ padding:"6px 4px" }}/></>}
                  {has3&&<><td style={{ padding:"6px 4px" }}/><td style={{ padding:"6px 4px" }}/></>}
                  <td style={{ padding:"6px 4px",fontFamily:FM,color:AL,textAlign:"right",fontSize:9 }}>{catSummary.reduce((a,c)=>a+c.codes,0).toLocaleString()}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div style={{ display:"flex",gap:10,padding:"6px 0 2px",fontSize:9,color:AL,flexWrap:"wrap" }}>
            <span>Click category → filter code list below</span>
            {mixAdj&&<span style={{ fontStyle:"italic" }}>Mix-adjusted: national avg scaled by enrollment demographics (children, disabled, aged share)</span>}
          </div>
        </div></Card>; })()}

        <div style={{ display:"flex",gap:3,alignItems:"center",justifyContent:"space-between",flexWrap:"wrap" }}>
          <div style={{ display:"flex",gap:3,alignItems:"center" }}>
            <span style={{ fontSize:8,color:AL,fontFamily:FM,textTransform:"uppercase",letterSpacing:0.5,marginRight:2 }}>Code Detail · Sort by</span>
            {[["fiscal","Largest $ Impact"],["high","Highest Rate"],["gap","Biggest % Gap"]].map(([k,l])=><Pill key={k} on={bSort===k} onClick={()=>setBS(k)}>{l}</Pill>)}
          </div>
          <span style={{ fontSize:9,color:AL }}>{bench.length} codes{bCat!=="All"?` in ${bCat}`:""}{q?` matching "${q}"`:""}</span>
        </div>
        {/* Scatter: Volume × Gap — shows where the money is */}
        {bench.length>3 && (() => {
          const s1Data = bench.filter(d=>d.nc>0&&Math.abs(d.g1)<500);
          const s2Data = has2 ? bench.filter(d=>d.nc>0&&d.g2!=null&&Math.abs(d.g2)<500).map(d=>({...d,g1:d.g2,_r:d.r2,_st:states[s2]?.name||s2})) : [];
          const s3Data = has3 ? bench.filter(d=>d.nc>0&&d.g3!=null&&Math.abs(d.g3)<500).map(d=>({...d,g1:d.g3,_r:d.r3,_st:states[s3]?.name||s3})) : [];
          return <Card x><CH t="Where the Money Is" b={`Rate gap × claims volume${mixAdj?" (mix-adjusted)":""}`} r={compLabel}/><div style={{ padding:"0 14px 8px" }}>
          <ResponsiveContainer width="100%" height={300}>
            <ScatterChart margin={{top:10,right:20,bottom:5,left:10}}>
              <CartesianGrid strokeDasharray="3 3" stroke={B}/>
              <XAxis type="number" dataKey="nc" name="Claims" scale="log" domain={["auto","auto"]} tick={{fill:AL,fontSize:8,fontFamily:FM}} axisLine={false} tickLine={false} tickFormatter={v=>v>=1e9?`${(v/1e9).toFixed(0)}B`:v>=1e6?`${(v/1e6).toFixed(0)}M`:v>=1e3?`${(v/1e3).toFixed(0)}K`:String(v)} label={{value:"Claims Volume →",position:"bottom",fontSize:9,fill:AL,offset:-2}}/>
              <YAxis type="number" dataKey="g1" name="Gap" domain={([a,b])=>[Math.max(a,-200),Math.min(b,200)]} tick={{fill:AL,fontSize:8,fontFamily:FM}} axisLine={false} tickLine={false} tickFormatter={v=>`${v>0?"+":""}${v.toFixed(0)}%`} label={{value:`Gap vs ${mixAdj?"Adj ":""}Natl Avg →`,angle:-90,position:"left",fontSize:9,fill:AL}}/>
              <ReferenceLine y={0} stroke={AL} strokeDasharray="4 4"/>
              <Tooltip content={<SafeTip render={(_d)=>{
                const d = _d as {c:string;d:string;_st?:string;_r?:number;naRef:number;g1:number;nc:number;r1:number;fi:number;mc:number;fs1:number};
                if(d._st) return (<div>
                  <div style={{fontWeight:600}}>{d.c} — {d.d}</div>
                  <div>{d._st}: <b>{f$(d._r ?? 0)}</b> · {mixAdj?"Adj ":""}Natl: {f$(d.naRef)}</div>
                  <div style={{color:d.g1>0?POS:NEG}}>Gap: {d.g1>0?"+":""}{d.g1.toFixed(1)}%</div>
                  <div style={{color:AL,fontSize:9}}>Volume: {fN(d.nc)} claims</div>
                </div>);
                return d.c?(<div>
                  <div style={{fontWeight:600}}>{d.c} — {d.d}</div>
                  <div>{states[s1]?.name||s1}: <b>{f$(d.r1)}</b> · {mixAdj?"Adj ":""}Natl: {f$(d.naRef)}</div>
                  <div style={{color:d.g1>0?POS:NEG}}>Gap: {d.g1>0?"+":""}{d.g1.toFixed(1)}%</div>
                  <div style={{color:AL,fontSize:9}}>Volume: {fN(d.nc)} claims · Impact: {f$(Math.abs(d.fi))}</div>
                  {d.mc>0&&<div style={{fontSize:9}}>Medicare: {f$(d.mc)}{d.fs1>0?` · Fee Sched: ${f$(d.fs1)}`:""}</div>}
                </div>):null;
              }}/>}/>
              <Scatter data={s1Data} fill={POS} name={states[s1]?.name||s1}>
                {s1Data.map((d,i)=><Cell key={i} fill={d.g1>0?POS:NEG} opacity={Math.min(0.35+Math.abs(d.fi)/1e8,0.85)} r={Math.max(3,Math.min(Math.sqrt(Math.abs(d.fi)/1e5),12))}/>)}
              </Scatter>
              {has2&&<Scatter data={s2Data} fill="#C4590A" name={states[s2]?.name||s2}>
                {s2Data.map((d,i)=><Cell key={i} fill="#C4590A" opacity={0.45} r={4}/>)}
              </Scatter>}
              {has3&&<Scatter data={s3Data} fill="#1565C0" name={states[s3]?.name||s3}>
                {s3Data.map((d,i)=><Cell key={i} fill="#1565C0" opacity={0.45} r={4}/>)}
              </Scatter>}
            </ScatterChart>
          </ResponsiveContainer>
          <div style={{ display:"flex",gap:12,fontSize:9,color:AL,padding:"2px 0",flexWrap:"wrap" }}>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:POS,verticalAlign:"middle",marginRight:3 }}/>{states[s1]?.name||s1} (above avg)</span>
            <span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:NEG,verticalAlign:"middle",marginRight:3 }}/>{states[s1]?.name||s1} (below avg)</span>
            {has2&&<span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:"#C4590A",verticalAlign:"middle",marginRight:3,opacity:0.6 }}/>{states[s2]?.name||s2}</span>}
            {has3&&<span><span style={{ display:"inline-block",width:8,height:8,borderRadius:"50%",background:"#1565C0",verticalAlign:"middle",marginRight:3,opacity:0.6 }}/>{states[s3]?.name||s3}</span>}
            <span>Dot size = fiscal impact · Top-right = overpays high-volume · Bottom-right = underpays high-volume</span>
          </div>
        </div></Card>;
        })()}
        <Card x><CH t="Rate Table" b={compLabel} r={`${bench.length} codes`}/><div style={{ padding:"0 14px 8px",overflowX:"auto",maxHeight:600,overflowY:"auto" }}>
          <table style={{ width:"100%",borderCollapse:"collapse",fontSize:10 }}><thead><tr style={{ borderBottom:`2px solid ${B}` }}>
            {[
              {h:"Code",t:"HCPCS",show:true},
              {h:"Desc",t:"Description",show:true},
              {h:"Cat",t:"Category",show:true},
              {h:states[s1]?.name||s1,t:`${states[s1]?.name||s1} avg $/claim (T-MSIS)`,show:true},
              {h:states[s2]?.name||s2,t:`${states[s2]?.name||s2} avg $/claim`,show:has2},
              {h:states[s3]?.name||s3,t:`${states[s3]?.name||s3} avg $/claim`,show:has3},
              {h:mixAdj?"Natl (adj)":"Natl",t:mixAdj?"National avg adjusted for enrollment mix":"National avg $/claim (T-MSIS)",show:true},
              {h:"Medicare",t:"Medicare PFS non-facility rate",show:!!mcRates},
              {h:"Fee Sched",t:`${states[s1]?.name||s1} Medicaid fee schedule rate`,show:!!feeScheds?.states?.[s1]},
              {h:"Gap",t:`${states[s1]?.name||s1} % above/below ${mixAdj?"adj ":""}natl avg`,show:true},
              {h:"Impact",t:"Est. $ difference if state paid natl avg (+ = state currently underpays)",show:true}
            ].filter(x=>x.show).map(({h,t})=><th key={h} title={t} style={{ textAlign:"left",padding:"6px 4px",color:AL,fontWeight:600,fontSize:8,textTransform:"uppercase",fontFamily:FM }}>{h}</th>)}
          </tr></thead><tbody>
            {bench.map(d=>(
              <tr key={d.c} style={{ borderBottom:`1px solid ${B}`,cursor:"pointer" }} onClick={()=>{setDC(d.c);setTab("code");}}>
                <td style={{ padding:"4px",fontFamily:FM,fontWeight:600 }}>{d.c}</td>
                <td title={d.d} style={{ padding:"4px",color:AL,maxWidth:120,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{d.d}</td>
                <td><Bdg>{d.cat}</Bdg></td>
                <td style={{ fontFamily:FM,fontWeight:600 }}>{f$(d.r1)}</td>
                {has2&&<td style={{ fontFamily:FM,color:AL }}>{d.r2>0?f$(d.r2):<span style={{color:B}}>—</span>}</td>}
                {has3&&<td style={{ fontFamily:FM,color:AL }}>{d.r3>0?f$(d.r3):<span style={{color:B}}>—</span>}</td>}
                <td style={{ fontFamily:FM,color:AL }}>{f$(d.naRef)}</td>
                {mcRates&&<td style={{ fontFamily:FM,color:d.mc>0?AL:B }}>{d.mc>0?f$(d.mc):"—"}</td>}
                {feeScheds?.states?.[s1]&&<td style={{ fontFamily:FM,color:d.fs1>0?"#1565C0":B }}>{d.fs1>0?f$(d.fs1):"—"}</td>}
                <td><span style={{ fontFamily:FM,fontWeight:700,color:d.g1<-10?NEG:d.g1>10?POS:AL }}>{d.g1>0?"+":""}{d.g1.toFixed(1)}%</span></td>
                <td style={{ fontFamily:FM,fontSize:9,color:d.fi>0?POS:NEG }}>{d.fi>0?"+":"\u2212"}{f$(Math.abs(d.fi))}</td>
              </tr>
            ))}
          </tbody></table>
          <div style={{ fontSize:9,color:AL,marginTop:4 }}>Click row → Code Profile · T-MSIS rates = <b>avg paid per claim</b>{mcRates?" · Medicare = PFS non-fac":""}{feeScheds?.states?.[s1]?" · Fee Sched = state Medicaid fee schedule":""}</div>
        </div></Card>
      </div>; })()}

      {/* CODE PROFILE */}
      {tab==="code" && <div style={{ display:"grid",gap:10 }}>
        <TabGuide title="Code Profile" desc="Deep dive into a single HCPCS code. See how every state's rate compares, review historical trends, and check Medicare and fee schedule benchmarks." tips="Search or click any code from the Rate Engine table to load its profile."/>
        <Card><div style={{ padding:"14px 20px" }}>
          <div style={{ display:"flex",gap:8,alignItems:"center",flexWrap:"wrap",marginBottom:6 }}>
            <button onClick={()=>setTab("rate")} style={{ fontSize:10,color:cB,background:"none",border:`1px solid ${B}`,borderRadius:5,padding:"3px 8px",cursor:"pointer" }}>← Rate Engine</button>
            <div style={{ flex:1 }}><CodeSearch codes={codes} value={dc} onChange={setDC}/></div>
            {dC && <ExportBtn onClick={()=>{
              downloadCSV(`code_${dC.c}_rates.csv`,["State","Rate","Gap vs Natl %"],dCS.map(d=>[d.n,d.r.toFixed(2),d.gp.toFixed(1)]));
            }}/>}
          </div>
          {dC && <div style={{ display:"flex",gap:6,alignItems:"center" }}><span style={{ fontSize:16,fontWeight:700,fontFamily:FM }}>{dC.c}</span><span style={{ fontSize:12,color:AL }}>{dC.d}</span><Bdg>{dC.cat}</Bdg></div>}
        </div>
        {dC && <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(90px,1fr))",padding:"0 10px 10px",gap:4 }}>
          <Met l="Natl Avg" v={f$(dC.na)}/><Met l="Claims" v={fN(dC.nc)}/><Met l="Natl Spend" v={f$(dC.ns||safe(dC.na)*safe(dC.nc))}/><Met l="States" v={dC.nst||Object.keys(dC.r||{}).length}/>
          {(dC.np ?? 0)>0&&<Met l="Provs" v={fN(dC.np ?? 0)}/>}{dC.cn&&<Fragment><Met l="Top1%" v={`${dC.cn.t1}%`} cl={dC.cn.t1>30?NEG:POS}/><Met l="Gini" v={safe(dC.cn.gi).toFixed(2)}/></Fragment>}
          {getMcRate(dC.c)>0&&<Met l="Medicare" v={f$(getMcRate(dC.c))} cl="#1565C0"/>}
          {getFsRate(s1,dC.c)>0&&<Met l={`${states[s1]?.name||s1} Sched`} v={f$(getFsRate(s1,dC.c))} cl="#6A1B9A"/>}
        </div>}</Card>
        {dC && dCS.length>0 && <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:10 }}>
          <Card x><CH t="Rates by State" b={`${dCS.length} states${getMcRate(dC.c)>0?" · blue = Medicare":""} · red = natl avg`}/><div style={{ padding:"0 14px 8px",maxHeight:500,overflowY:"auto" }}>
            <ResponsiveContainer width="100%" height={Math.max(200,Math.min(dCS.length*14,700))}>
              <BarChart data={dCS} layout="vertical" margin={{left:28,right:16}}>
                <CartesianGrid strokeDasharray="3 3" stroke={B} horizontal={false}/>
                <XAxis type="number" tick={{fill:AL,fontSize:9,fontFamily:FM}} axisLine={false} tickLine={false} tickFormatter={v=>f$(v)}/>
                <YAxis type="category" dataKey="ab" tick={{fill:A,fontSize:7,fontFamily:FM}} axisLine={false} tickLine={false} width={26}/>
                <ReferenceLine x={dC.na} stroke={NEG} strokeDasharray="4 2" strokeWidth={1.5}/>
                {getMcRate(dC.c)>0&&<ReferenceLine x={getMcRate(dC.c)} stroke="#1565C0" strokeDasharray="6 3" strokeWidth={1.5}/>}
                <Tooltip content={<SafeTip render={(_d)=>{ const d=_d as {n:string;r:number;gp:number}; return (<div><div style={{fontWeight:600}}>{d.n}: {f$(d.r)}</div><div style={{color:d.gp<0?NEG:POS}}>{d.gp>0?"+":""}{d.gp.toFixed(1)}% vs natl avg of {f$(dC.na)}</div>{getMcRate(dC.c)>0&&<div style={{color:"#1565C0"}}>Medicare: {f$(getMcRate(dC.c))} ({pD(d.r,getMcRate(dC.c))>0?"+":""}{pD(d.r,getMcRate(dC.c)).toFixed(0)}%)</div>}</div>); }}/>}/>
                <Bar dataKey="r" barSize={8} radius={[0,3,3,0]}>{dCS.map((d,i)=><Cell key={i} fill={d.ab===s1?cO:d.ab===s2?WARN:d.gp<0?"rgba(164,38,44,0.35)":"rgba(46,107,74,0.4)"}/>)}</Bar>
              </BarChart>
            </ResponsiveContainer>
          </div></Card>
          <div style={{ display:"grid",gap:8 }}>
            {dC.tr && <Card x><CH t="Trend" b={`${dC.tr.length}Y`}/><div style={{ padding:"0 14px 8px" }}>
              <ResponsiveContainer width="100%" height={140}>
                <AreaChart data={dC.tr} margin={{right:20}}><CartesianGrid strokeDasharray="3 3" stroke={B} vertical={false}/><XAxis dataKey="y" tick={{fill:AL,fontSize:9}} axisLine={false} tickLine={false} interval={0}/><YAxis tick={{fill:AL,fontSize:9}} axisLine={false} tickLine={false}/><Tooltip content={<SafeTip render={(_d)=>{ const d=_d as {y:number;v:number}; return <div>{d.y}: <b>{f$(d.v)}</b>/claim</div>; }}/>}/><Area type="monotone" dataKey="v" stroke={cB} strokeWidth={2} fill="rgba(46,107,74,0.08)" dot={{fill:cB,r:2.5}}/></AreaChart>
              </ResponsiveContainer>
            </div></Card>}
            {dC.cn && <Card x><CH t="Concentration"/><div style={{ padding:"6px 14px 10px" }}>
              {([["Top 1%",dC.cn.t1],["Top 5%",dC.cn.t5],["Top 10%",dC.cn.t10]] as [string, number][]).map(([l,v])=>(
                <div key={l} style={{ display:"flex",alignItems:"center",gap:8,marginBottom:6 }}>
                  <span style={{ fontSize:10,color:AL,width:48 }}>{l}</span>
                  <div style={{ flex:1,height:12,background:S,borderRadius:4,overflow:"hidden" }}><div style={{ width:`${safe(v)}%`,height:"100%",background:v>40?NEG:v>25?cO:POS,borderRadius:4 }}/></div>
                  <span style={{ fontFamily:FM,fontSize:11,fontWeight:700,width:40,textAlign:"right" }}>{safe(v)}%</span>
                </div>
              ))}
            </div></Card>}
            {!dC.cn&&!dC.tr&&<Card><div style={{ padding:"14px",fontSize:11,color:AL }}>{isLive?"Extended data not available for this code.":"Extended data available with pipeline."}</div></Card>}
          </div>
        </div>}
      </div>}

      {/* PROVIDERS */}
      {tab==="provider" && <div style={{ display:"grid",gap:10 }}>
        <div style={{ display:"flex",gap:8,alignItems:"flex-start",justifyContent:"space-between",flexWrap:"wrap" }}>
          <TabGuide title="Providers" desc="Search top providers by state — see their total billing, codes used, and peer comparisons. Switch to 'By Specialty' to see which taxonomy groups drive spending." tips="Requires pipeline data with NPPES integration. Search by name, NPI, or specialty."/>
          <ExportBtn onClick={()=>{
            if(provMode==="providers"&&providerData){
              const pql=pq.toLowerCase();
              const rows=providerData.filter(p=>(s1==="US"||p.state===s1)&&(!pql||(p.name||"").toLowerCase().includes(pql)||(p.npi||"").includes(pql))).slice(0,200);
              downloadCSV(`providers_${s1}.csv`,["NPI","Name","State","Specialty","Total Paid","Claims","Codes"],rows.map(p=>[p.npi,p.name,p.state||"",p.taxonomy||"",safe(p.paid),safe(p.claims),safe(p["codes"] as number | null | undefined)]));
            }
          }}/>
        </div>
        <div style={{ display:"flex",gap:8,alignItems:"center",flexWrap:"wrap" }}>
          <Sel value={s1} onChange={setS1} label="State"/>
          <div style={{ display:"flex",gap:3 }}>
            <Pill on={provMode==="providers"} onClick={()=>setPM("providers")}>Individual Providers</Pill>
            <Pill on={provMode==="specialties"} onClick={()=>setPM("specialties")}>By Specialty</Pill>
          </div>
          {provMode==="providers" && <div style={{ position:"relative",flex:1,maxWidth:280 }}>
            <input value={pq} onChange={e=>setPQ(e.currentTarget.value)} placeholder="Search by name, NPI, or specialty..." style={{ width:"100%",background:S,border:`1px solid ${B}`,padding:"6px 10px 6px 24px",borderRadius:6,fontSize:11,outline:"none",boxSizing:"border-box" }}/>
            <span style={{ position:"absolute",left:7,top:"50%",transform:"translateY(-50%)",color:AL,fontSize:12 }}>&#x2315;</span>
          </div>}
          {provMode==="specialties" && <div style={{ position:"relative",flex:1,maxWidth:280 }}>
            <input value={specQuery} onChange={e=>setSpecQuery(e.currentTarget.value)} placeholder="Search taxonomy code or keyword..." style={{ width:"100%",background:S,border:`1px solid ${B}`,padding:"6px 10px 6px 24px",borderRadius:6,fontSize:11,outline:"none",boxSizing:"border-box" }}/>
            <span style={{ position:"absolute",left:7,top:"50%",transform:"translateY(-50%)",color:AL,fontSize:12 }}>&#x2315;</span>
          </div>}
        </div>

        {/* INDIVIDUAL PROVIDERS MODE */}
        {provMode==="providers" && (() => {
          type ProvExt = ProviderRecord & { type?: string; n_codes?: number; total_claims?: number; total_paid?: number; total_bene?: number; peer?: { n_peers: number; med_paid: number; vs_med: number; med_claims: number }; trend?: { y: number; paid: number; claims: number }[]; category_shares?: Record<string, number>; top_codes?: { code: string; desc?: string; paid?: number; share?: number }[] };
          if (!providerData || providerData.length === 0) return <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:12 }}>
            <div style={{ fontSize:14,marginBottom:4 }}>Provider profiles available with pipeline + NPPES</div>
            <div>Run the pipeline with NPPES to see top 200 providers per state with case mix, trends, and peer comparison.</div>
          </div></Card>;
          const pql = pq.toLowerCase();
          const filtered = (providerData as ProvExt[]).filter(p => {
            if (s1 !== "US" && p.state !== s1) return false;
            if (!pql) return true;
            return (p.name||"").toLowerCase().includes(pql) || (p.npi||"").includes(pql) || (p.taxonomy||"").toLowerCase().includes(pql);
          }).slice(0, 60);
          const selProv = (selNpi && filtered.find(p => p.npi === selNpi)) || filtered[0];
          return <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:10 }}>
            <Card x><CH t={`Providers${s1!=="US"?" in "+(states[s1]?.name||s1):""}`} b={`${filtered.length}${filtered.length>=60?"+":""} results`}/><div style={{ padding:"0 14px 8px",maxHeight:440,overflowY:"auto" }}>
              {filtered.map(p => {
                const isSel = selProv && selProv.npi === p.npi;
                return <div key={p.npi} style={{ display:"flex",alignItems:"center",gap:6,padding:"5px 4px",borderBottom:`1px solid ${S}`,cursor:"pointer",background:isSel?"rgba(46,107,74,0.06)":"transparent",borderRadius:4 }} onClick={()=>setSelNpi(p.npi)}>
                  <div style={{ width:24,height:24,borderRadius:12,background:p.type==="org"?cT:cB,display:"flex",alignItems:"center",justifyContent:"center",color:"#fff",fontSize:10,fontWeight:700,flexShrink:0 }}>{p.type==="org"?"O":"I"}</div>
                  <div style={{ flex:1,minWidth:0 }}>
                    <div title={p.name||`NPI ${p.npi}`} style={{ fontSize:11,fontWeight:isSel?600:400,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis" }}>{p.name||`NPI ${p.npi}`}</div>
                    <div style={{ fontSize:9,color:AL }}>{p.state} · {p.n_codes ?? 0} codes · {fN(safe(p.total_claims))} claims</div>
                  </div>
                  <div style={{ textAlign:"right",flexShrink:0 }}>
                    <div style={{ fontSize:11,fontFamily:FM,fontWeight:600 }}>{f$(safe(p.total_paid))}</div>
                  </div>
                </div>;
              })}
              {filtered.length===0 && <div style={{ padding:12,fontSize:11,color:AL,textAlign:"center" }}>No providers match your search.</div>}
            </div></Card>
            {selProv && <div style={{ display:"grid",gap:8 }}>
              <Card accent={cB}><div style={{ padding:"10px 14px 8px" }}>
                <div style={{ fontSize:16,fontWeight:300 }}>{selProv.name||`NPI ${selProv.npi}`}</div>
                <div style={{ fontSize:10,color:AL,marginTop:2 }}>NPI: {selProv.npi} · {selProv.type==="org"?"Organization":"Individual"}{selProv.taxonomy?` · ${selProv.taxonomy}`:""}</div>
                <div style={{ display:"grid",gridTemplateColumns:"repeat(3,1fr)",marginTop:8,gap:4 }}>
                  <Met l="Total Paid" v={f$(safe(selProv.total_paid))}/><Met l="Claims" v={fN(safe(selProv.total_claims))}/><Met l="Beneficiaries" v={fN(safe(selProv.total_bene))}/>
                  <Met l="State" v={states[selProv.state || ""]?.name||selProv.state||""}/><Met l="Codes Billed" v={selProv.n_codes ?? 0}/><Met l="Avg/Claim" v={(selProv.total_claims ?? 0)>0?`$${((selProv.total_paid ?? 0)/(selProv.total_claims ?? 1)).toFixed(2)}`:"—"}/>
                </div>
                {selProv.peer && <div style={{ marginTop:6,padding:"6px 0 0",borderTop:`1px solid ${S}` }}>
                  <div style={{ fontSize:10,color:AL,marginBottom:3 }}>Peer Comparison ({selProv.peer.n_peers} peers in {states[selProv.state || ""]?.name||selProv.state||""} with same taxonomy)</div>
                  <div style={{ display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:4 }}>
                    <Met l="Peer Median" v={f$(safe(selProv.peer.med_paid))}/>
                    <Met l="vs Median" v={<span style={{color:selProv.peer.vs_med>0?NEG:POS}}>{selProv.peer.vs_med>0?"+":""}{selProv.peer.vs_med}%</span>}/>
                    <Met l="Peer Claims" v={fN(safe(selProv.peer.med_claims))}/>
                  </div>
                </div>}
              </div></Card>
              {selProv.trend && selProv.trend.length>1 && <Card x><CH t="Yearly Spending" b={`${selProv.trend.length} years`}/><div style={{ padding:"0 14px 8px" }}>
                <ResponsiveContainer width="100%" height={120}>
                  <AreaChart data={selProv.trend} margin={{right:20}}><CartesianGrid strokeDasharray="3 3" stroke={B} vertical={false}/><XAxis dataKey="y" tick={{fill:AL,fontSize:9}} axisLine={false} tickLine={false} interval={0}/><YAxis tick={{fill:AL,fontSize:9}} axisLine={false} tickLine={false} tickFormatter={v=>f$(v)}/><Tooltip content={<SafeTip render={(_d)=>{ const d=_d as {y:number;paid:number;claims:number}; return <div>{d.y}: <b>{f$(d.paid)}</b> · {fN(safe(d.claims))} claims</div>; }}/>}/><Area type="monotone" dataKey="paid" stroke={cB} strokeWidth={2} fill="rgba(46,107,74,0.08)" dot={{fill:cB,r:2.5}}/></AreaChart>
                </ResponsiveContainer>
              </div></Card>}
              {selProv.category_shares && <Card x><CH t="Case Mix" b="Spending by service category"/><div style={{ padding:"6px 14px 10px" }}>
                {Object.entries(selProv.category_shares).slice(0,8).map(([cat,pct])=>(
                  <div key={cat} style={{ display:"flex",alignItems:"center",gap:8,marginBottom:5 }}>
                    <span style={{ fontSize:10,color:AL,width:80,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis" }}>{cat}</span>
                    <div style={{ flex:1,height:12,background:S,borderRadius:4,overflow:"hidden" }}><div style={{ width:`${Math.min(safe(pct),100)}%`,height:"100%",background:cB,borderRadius:4,opacity:0.7 }}/></div>
                    <span style={{ fontFamily:FM,fontSize:11,fontWeight:700,width:40,textAlign:"right" }}>{safe(pct).toFixed(1)}%</span>
                  </div>
                ))}
              </div></Card>}
              {selProv.top_codes && selProv.top_codes.length>0 && <Card x><CH t="Top Codes" b={`${selProv.top_codes.length} highest-spend procedures`}/><div style={{ padding:"0 14px 8px" }}>
                {selProv.top_codes.map((tc,i)=>(
                  <div key={i} style={{ display:"flex",alignItems:"center",gap:6,padding:"3px 0",borderBottom:`1px solid ${S}`,fontSize:10 }}>
                    <span style={{ fontFamily:FM,fontWeight:600,width:50,color:cB }}>{tc.code}</span>
                    <span title={tc.desc||tc.code} style={{ flex:1,color:A,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap" }}>{tc.desc||tc.code}</span>
                    <span style={{ fontFamily:FM,width:55,textAlign:"right" }}>{f$(safe(tc.paid))}</span>
                    <span style={{ fontFamily:FM,width:35,textAlign:"right",color:AL }}>{safe(tc.share)}%</span>
                  </div>
                ))}
              </div></Card>}
            </div>}
          </div>;
        })()}

        {/* SPECIALTIES MODE */}
        {provMode==="specialties" && (() => {
          type SpecState = { state: string; avg_per_prov: number; avg_per_claim: number; provs: number };
          type SpecExt = SpecialtyRecord & { n_states?: number; national_providers?: number; national_paid?: number; states?: SpecState[] };
          if (!specData || specData.length === 0) return <Card><div style={{ padding:20,textAlign:"center",color:AL,fontSize:12 }}>
            <div style={{ fontSize:14,marginBottom:4 }}>Specialty comparison available with pipeline + NPPES</div>
            <div>Run the pipeline with NPPES to see spending by provider taxonomy across all states.</div>
          </div></Card>;
          const sql = specQuery.toLowerCase();
          const filtSpec = (specData as SpecExt[]).filter(sp => !sql || sp.taxonomy.toLowerCase().includes(sql)).slice(0, 50);
          const selSpec = (selSpecTax && filtSpec.find(sp => sp.taxonomy === selSpecTax)) || filtSpec[0];
          const selStates = selSpec?.states ? [...selSpec.states].sort((a,b)=>b.avg_per_prov-a.avg_per_prov) : [];
          const maxPP = selStates.length > 0 ? selStates[0].avg_per_prov : 1;
          return <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:10 }}>
            <Card x><CH t="Specialties" b={`${filtSpec.length} taxonomy codes`}/><div style={{ padding:"0 14px 8px",maxHeight:440,overflowY:"auto" }}>
              {filtSpec.map(sp => {
                const isSel = selSpec && selSpec.taxonomy === sp.taxonomy;
                return <div key={sp.taxonomy} style={{ display:"flex",alignItems:"center",gap:6,padding:"5px 4px",borderBottom:`1px solid ${S}`,cursor:"pointer",background:isSel?"rgba(46,107,74,0.06)":"transparent",borderRadius:4 }} onClick={()=>setSelSpecTax(sp.taxonomy)}>
                  <div style={{ flex:1,minWidth:0 }}>
                    <div style={{ fontSize:11,fontWeight:isSel?600:400,fontFamily:FM }}>{sp.taxonomy}</div>
                    <div style={{ fontSize:9,color:AL }}>{sp.n_states ?? 0} states · {fN(safe(sp.national_providers))} providers</div>
                  </div>
                  <div style={{ textAlign:"right",flexShrink:0 }}>
                    <div style={{ fontSize:11,fontFamily:FM,fontWeight:600 }}>{f$(safe(sp.national_paid))}</div>
                  </div>
                </div>;
              })}
            </div></Card>
            {selSpec && <div style={{ display:"grid",gap:8 }}>
              <Card accent={cT}><div style={{ padding:"10px 14px 8px" }}>
                <div style={{ fontSize:16,fontWeight:300,fontFamily:FM }}>{selSpec.taxonomy}</div>
                <div style={{ display:"grid",gridTemplateColumns:"repeat(3,1fr)",marginTop:8,gap:4 }}>
                  <Met l="Total Paid" v={f$(safe(selSpec.national_paid))}/><Met l="Providers" v={fN(safe(selSpec.national_providers))}/><Met l="States" v={selSpec.n_states ?? 0}/>
                </div>
              </div></Card>
              <Card x><CH t="Cross-State Comparison" b="Avg spending per provider"/><div style={{ padding:"0 14px 8px",maxHeight:320,overflowY:"auto" }}>
                {selStates.map((st,i)=>(
                  <div key={i} style={{ display:"flex",alignItems:"center",gap:6,padding:"3px 0",fontSize:10 }}>
                    <span style={{ width:20,fontFamily:FM,color:AL,fontSize:8,textAlign:"right" }}>{i+1}</span>
                    <span style={{ width:70,fontWeight:st.state===s1?600:400 }}>{states[st.state]?.name||st.state}</span>
                    <div style={{ flex:1,height:8,background:S,borderRadius:3 }}><div style={{ width:`${(safe(st.avg_per_prov)/maxPP)*100}%`,height:"100%",background:st.state===s1?cO:cB,borderRadius:3,opacity:0.7 }}/></div>
                    <span style={{ fontFamily:FM,width:55,textAlign:"right" }}>{f$(safe(st.avg_per_prov))}</span>
                    <span style={{ fontFamily:FM,width:35,textAlign:"right",color:AL }}>{st.provs}p</span>
                  </div>
                ))}
              </div></Card>
              <Card x><CH t="Rate per Claim" b="Cross-state comparison"/><div style={{ padding:"0 14px 8px" }}>
                <ResponsiveContainer width="100%" height={160}>
                  <BarChart data={selStates.slice(0,20).map(st=>({n:(states[st.state]?.name||st.state).substring(0,8),v:safe(st.avg_per_claim),st:st.state}))} margin={{left:5}}>
                    <CartesianGrid strokeDasharray="3 3" stroke={B} horizontal={true} vertical={false}/>
                    <XAxis dataKey="n" tick={{fill:AL,fontSize:8}} axisLine={false} tickLine={false} interval={0} angle={-30} textAnchor="end" height={40}/>
                    <YAxis tick={{fill:AL,fontSize:9,fontFamily:FM}} axisLine={false} tickLine={false} tickFormatter={v=>`$${v}`}/>
                    <Tooltip content={<SafeTip render={(_d)=>{ const d=_d as {st:string;v:number}; return <div>{states[d.st]?.name||d.st}: <b>${safe(d.v).toFixed(2)}</b>/claim</div>; }}/>}/>
                    <Bar dataKey="v" radius={[3,3,0,0]}>{selStates.slice(0,20).map(st=><Cell key={st.state} fill={st.state===s1?cO:cB} opacity={0.7}/>)}</Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div></Card>
            </div>}
          </div>;
        })()}
      </div>}

      {/* SIMULATOR */}
      {tab==="sim" && <div style={{ display:"grid",gap:10 }}>
        <div style={{ display:"flex",gap:8,alignItems:"flex-start",justifyContent:"space-between",flexWrap:"wrap" }}>
          <TabGuide title="Rate Impact Simulator" desc="Model what happens if you raise or lower Medicaid rates. Pick a state, category, and percentage change to see projected fiscal impact split by FFS and managed care." tips="The 'If You Used Their Rates' table shows what your state would spend at another state's rate levels — useful for rate-setting benchmarking."/>
          <ExportBtn onClick={()=>{
            const stCodes2=codes.filter(h=>h.r&&h.r[simState]!==undefined&&(simCat==="All"||h.cat===simCat));
            const totalNS=SL.reduce((a,k)=>a+safe(states[k]?.spend),0);
            const stSh=totalNS>0?(states[simState]?.spend||0)/totalNS:0;
            const rows=stCodes2.map(h=>{const r=safe(h.r[simState]);const ec=safe(h.nc)*stSh;return [h.c,h.d,h.cat,r.toFixed(2),(r*(1+simPct/100)).toFixed(2),(r*simPct/100*ec).toFixed(0)];}).filter(r=>r[3]!=="0.00");
            downloadCSV(`sim_${simState}_${simPct}pct.csv`,["Code","Description","Category","Current Rate","New Rate","Impact $"],rows);
          }}/>
        </div>
        <Card accent={cO}><div style={{ padding:"12px 16px 10px" }}>
          <div style={{ fontSize:14,fontWeight:300,marginBottom:8 }}>Rate Impact Simulator</div>
          <div style={{ display:"flex",gap:10,alignItems:"center",flexWrap:"wrap" }}>
            <div><span style={{ fontSize:10,color:AL }}>State</span><br/>
              <select value={simState} onChange={e=>setSimSt(e.currentTarget.value)} style={{ background:S,border:`1px solid ${B}`,padding:"5px 8px",borderRadius:6,fontSize:11 }}>
                {SL.map(k=><option key={k} value={k}>{states[k]?.name||k}</option>)}
              </select>
            </div>
            <div><span style={{ fontSize:10,color:AL }}>Category</span><br/>
              <select value={simCat} onChange={e=>setSimCat(e.currentTarget.value)} style={{ background:S,border:`1px solid ${B}`,padding:"5px 8px",borderRadius:6,fontSize:11 }}>
                <option value="All">All Codes</option>
                {[...new Set(codes.map(h=>h.cat))].sort().map(c=><option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div><span style={{ fontSize:10,color:AL }}>Rate Change</span><br/>
              <div style={{ display:"flex",alignItems:"center",gap:4 }}>
                <input type="range" min={-50} max={50} value={simPct} onChange={e=>setSimPct(+e.currentTarget.value)} style={{ width:120 }}/>
                <span style={{ fontFamily:FM,fontSize:14,fontWeight:700,color:simPct>=0?POS:NEG,minWidth:45 }}>{simPct>0?"+":""}{simPct}%</span>
              </div>
            </div>
          </div>
        </div></Card>
        {(() => {
          // Use module-level FFS_SHARE constant
          const ffsShare = FFS_SHARE[simState] || 0.40;

          const stCodes = codes.filter(h => h.r && h.r[simState] !== undefined && (simCat==="All" || h.cat===simCat));
          const totalNatlSpend = SL.reduce((a,k)=>a+safe(states[k]?.spend),0);
          const stShare = totalNatlSpend > 0 ? (states[simState]?.spend || 0) / totalNatlSpend : 0;

          const affected = stCodes.map(h => {
            const curRate = safe(h.r[simState]);
            // Estimate state claims from national claims × state spending share
            // NOTE: This assumes uniform utilization mix. Real impact varies by code.
            const estClaims = safe(h.nc) * stShare;
            const curSpend = curRate * estClaims;
            const newSpend = curRate * (1 + simPct / 100) * estClaims;
            const delta = newSpend - curSpend;
            return { c: h.c, d: h.d, cat: h.cat, rate: curRate, na: safe(h.na), claims: estClaims, cur: curSpend, delta, newRate: curRate * (1 + simPct / 100) };
          }).filter(h => h.claims > 0 && h.rate > 0);
          const totalDelta = affected.reduce((a, h) => a + h.delta, 0);
          const totalCur = affected.reduce((a, h) => a + h.cur, 0);
          // FFS-adjusted: only the FFS share of claims is directly affected by rate changes
          const ffsDelta = totalDelta * ffsShare;
          const topImpact = [...affected].sort((a,b) => Math.abs(b.delta) - Math.abs(a.delta)).slice(0, 15);
          const catImpact: Record<string, number> = {};
          affected.forEach(h => { catImpact[h.cat] = (catImpact[h.cat] || 0) + h.delta; });
          const catSorted = Object.entries(catImpact).sort((a,b) => Math.abs(b[1]) - Math.abs(a[1]));
          const maxCatImp = catSorted.length > 0 ? Math.max(...catSorted.map(c => Math.abs(c[1]))) : 1;

          return <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:10 }}>
            <div style={{ display:"grid",gap:8 }}>
              <Card><div style={{ padding:"12px 16px" }}>
                <div style={{ fontSize:10,color:AL,textTransform:"uppercase",letterSpacing:1 }}>Projected Annual Impact</div>
                <div style={{ display:"grid",gridTemplateColumns:"1fr 1fr",gap:8,marginTop:6 }}>
                  <div>
                    <div style={{ fontSize:9,color:AL }}>All Claims (FFS + MC)</div>
                    <div style={{ fontSize:22,fontWeight:200,fontFamily:FM,color:totalDelta>=0?NEG:POS }}>{totalDelta>=0?"+":""}{f$(Math.abs(totalDelta))}</div>
                  </div>
                  <div>
                    <div style={{ fontSize:9,color:AL }}>FFS Only (~{(ffsShare*100).toFixed(0)}% of claims)</div>
                    <div style={{ fontSize:22,fontWeight:200,fontFamily:FM,color:ffsDelta>=0?NEG:POS }}>{ffsDelta>=0?"+":""}{f$(Math.abs(ffsDelta))}</div>
                  </div>
                </div>
                <div style={{ fontSize:10,color:AL,marginTop:6 }}>{simPct>=0?"increase":"decrease"} on {fN(affected.length)} codes in {states[simState]?.name||simState} · {simCat==="All"?"all categories":simCat}</div>
                <div style={{ fontSize:10,color:AL }}>Estimated total spend: {f$(totalCur)} · {states[simState]?.name||simState} is ~{(ffsShare*100).toFixed(0)}% FFS / ~{((1-ffsShare)*100).toFixed(0)}% managed care</div>
                <div style={{ background:"rgba(184,134,11,0.08)",border:`1px solid rgba(184,134,11,0.2)`,borderRadius:6,padding:"6px 8px",marginTop:8,fontSize:9,color:"#8B7000",lineHeight:1.6 }}>
                  <b>Methodology note:</b> State claims are estimated proportionally from national totals. Real utilization mix varies by state — high-PCS states will see larger PCS impacts, etc. FFS rate changes do not directly affect managed care capitation rates, though they may influence future actuarial rate-setting. The "FFS Only" figure approximates the direct fiscal impact of a fee schedule change.
                </div>
              </div></Card>
              <Card x><CH t="Impact by Category" b={`${catSorted.length} categories`}/><div style={{ padding:"6px 14px 10px" }}>
                {catSorted.slice(0,10).map(([cat,d])=>(
                  <div key={cat} style={{ display:"flex",alignItems:"center",gap:8,marginBottom:5 }}>
                    <span style={{ fontSize:10,color:AL,width:75,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis" }}>{cat}</span>
                    <div style={{ flex:1,height:10,background:S,borderRadius:4,position:"relative",overflow:"hidden" }}><div style={{ width:`${(Math.abs(d)/maxCatImp)*100}%`,height:"100%",background:d>=0?NEG:POS,borderRadius:4,opacity:0.6 }}/></div>
                    <span style={{ fontFamily:FM,fontSize:10,fontWeight:600,width:60,textAlign:"right",color:d>=0?NEG:POS }}>{d>=0?"+":"-"}{f$(Math.abs(d))}</span>
                  </div>
                ))}
              </div></Card>
              <Card x><CH t="If You Used Their Rates" b={`${states[simState]?.name||simState}'s volume at other states' rates`}/><div style={{ padding:"0 14px 8px" }}>
                {(() => {
                  const compStates = SL.filter(k=>k!==simState).map(k=>{
                    const compSpend = stCodes.reduce((a,h)=>{
                      const r=safe(h.r?.[k]);
                      return a+(r>0?r*safe(h.nc)*stShare:0);
                    },0);
                    return {k,n:states[k]?.name||k,spend:compSpend};
                  }).filter(s=>s.spend>0).sort((a,b)=>a.spend-b.spend);
                  const simNewSpend = totalCur + totalDelta;
                  const myIdx = compStates.findIndex(s=>s.spend>simNewSpend);
                  return <div style={{ maxHeight:160,overflowY:"auto" }}>
                    {compStates.slice(0,15).map((s,i)=>(
                      <div key={s.k} style={{ display:"flex",alignItems:"center",gap:4,fontSize:10,padding:"2px 0" }}>
                        <span style={{ width:60,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis" }}>{s.n}</span>
                        <span style={{ fontFamily:FM,width:55,textAlign:"right" }}>{f$(s.spend)}</span>
                      </div>
                    ))}
                    <div style={{ fontSize:9,color:AL,marginTop:4 }}>{states[simState]?.name} after change: {f$(simNewSpend)} (rank ~{myIdx>=0?myIdx+1:compStates.length+1} of {compStates.length+1})</div>
                  </div>;
                })()}
              </div></Card>
            </div>
            <Card x><CH t="Codes with Largest Impact" b={`Top ${topImpact.length} by absolute change`}/><div style={{ padding:"0 14px 8px",maxHeight:460,overflowY:"auto" }}>
              {topImpact.map((h,i)=>(
                <div key={i} style={{ display:"flex",alignItems:"center",gap:6,padding:"4px 0",borderBottom:`1px solid ${S}`,fontSize:10 }}>
                  <span style={{ fontFamily:FM,fontWeight:600,width:50,color:cB }}>{h.c}</span>
                  <div style={{ flex:1,minWidth:0 }}>
                    <div title={h.d} style={{ whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis" }}>{h.d}</div>
                    <div style={{ fontSize:9,color:AL }}>{h.cat} · {fN(Math.round(h.claims))} est. claims · ${h.rate.toFixed(2)} → ${h.newRate.toFixed(2)}</div>
                  </div>
                  <div style={{ textAlign:"right",flexShrink:0 }}>
                    <div style={{ fontFamily:FM,fontWeight:600,color:h.delta>=0?NEG:POS }}>{h.delta>=0?"+":"-"}{f$(Math.abs(h.delta))}</div>
                    <div style={{ fontSize:8,color:AL }}>vs natl avg: ${h.na.toFixed(2)}</div>
                  </div>
                </div>
              ))}
            </div></Card>
          </div>;
        })()}
      </div>}

      {/* ABOUT */}
      {tab==="about" && <div style={{ maxWidth:640,display:"grid",gap:10 }}>
        <Card><CH t="About This Tool"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
          {isLive ? <span>This dashboard is built from <b>HHS Medicaid Provider Spending data</b> published on opendata.hhs.gov. It covers {safe(meta?.source_rows as number | null | undefined).toLocaleString()} rows, {safe(meta?.n_codes as number | null | undefined).toLocaleString()} HCPCS codes across {safe(meta?.n_states as number | null | undefined)} jurisdictions{meta?.years?`, ${(meta.years as number[])[0]}–${(meta.years as number[])[(meta.years as number[]).length-1]}`:""}.
          Every number comes from actual Medicaid claims: NPI × HCPCS × month, aggregated through a DuckDB pipeline that runs locally. Nothing is estimated except per-enrollee figures, which use CMS enrollment data (Nov 2024, Medicaid only).</span>
          : <span>This prototype uses <b>simulated data</b> modeled on real HHS dataset structure. The numbers are directionally plausible but not from actual claims. Run the DuckDB pipeline with real HHS data to replace everything you see here with actuals.</span>}
        </div></Card>
        {/* Reference Data Status */}
        <Card><CH t="Reference Data" b="loaded alongside T-MSIS claims"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
          {mcRates ? <div style={{marginBottom:4}}><span style={{color:POS,fontWeight:600}}>✓ Medicare PFS</span> — CY{mcRates.year} Physician Fee Schedule ({Object.keys(mcRates.rates).length.toLocaleString()} codes, CF=${mcRates.cf}). Shows Medicare non-facility rates in Rate Engine and Code Profile for direct Medicaid-to-Medicare comparisons.</div>
          : <div style={{color:AL,marginBottom:4}}>○ Medicare PFS — not loaded. Place medicare_rates.json in /data to enable.</div>}
          {feeScheds ? <div style={{marginBottom:4}}><span style={{color:POS,fontWeight:600}}>✓ State Fee Schedules</span> — {Object.keys(feeScheds.states).map(k=>{ const st = feeScheds.states[k] as FeeScheduleState & {name?:string;n?:number;source?:string}; return `${st.name ?? k} (${st.n?.toLocaleString() ?? "?"} codes, ${st.source ?? "unknown"})`; }).join("; ")}. Shows official Medicaid fee schedule rates alongside T-MSIS actual-paid rates.</div>
          : <div style={{color:AL,marginBottom:4}}>○ Fee Schedules — not loaded. Place fee_schedules.json in /data to enable.</div>}
          {fsDir ? <div style={{marginBottom:4}}><span style={{color:POS,fontWeight:600}}>✓ Fee Schedule Directory</span> — Links to {fsDir.count} state Medicaid fee schedule pages (compiled {fsDir.compiled}).</div>
          : <div style={{color:AL,marginBottom:4}}>○ Fee Schedule Directory — not loaded. Place fee_schedule_directory.json in /data to enable.</div>}
          {riskAdj ? <div style={{marginBottom:4}}><span style={{color:POS,fontWeight:600}}>✓ Risk Adjustment</span> — Eligibility-mix adjustment for {Object.keys(riskAdj.states).length} states (source: {(riskAdj as RiskAdjData & {source?:string}).source ?? "unknown"}). Toggle "Raw/Mix-Adj" in Per Cap mode to see spending adjusted for state enrollment demographics.</div>
          : <div style={{color:AL,marginBottom:4}}>○ Risk Adjustment — not loaded. Place risk_adj.json in /data to enable.</div>}
        </div></Card>
        <Card><CH t="Methodology"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
          <b>T-MSIS rates:</b> total paid ÷ total claims, per code per state. What Medicaid actually paid, divided by how many times. No modifier weighting — rates are blended across all modifiers and places of service. These will differ from published fee schedule rates because they reflect actual payments including partial claims, copay reductions, MC encounter reporting, and claims edits.<br/>
          <b>Fee schedule rates:</b> Official Medicaid fee schedule rates as published by state agencies. For FL: the AHCA Practitioner Fee Schedule (FSI = non-facility rate). The gap between fee schedule and T-MSIS actual-paid reflects modifier mix, place-of-service distribution, managed care encounter pricing, and claims processing rules.<br/>
          <b>Medicare rates:</b> CMS Physician Fee Schedule non-facility rates (total RVU × conversion factor). Useful as a benchmark — most state Medicaid rates are set as a percentage of Medicare or derived from the same RBRVS methodology.<br/>
          <b>Risk adjustment:</b> Eligibility-mix adjustment using MACPAC data. For each state, expected per-enrollee spending is computed using national per-enrollee rates by eligibility group (child, new adult, other adult, disabled, aged) applied to the state's enrollment shares. The adjustment factor = expected ÷ national average. States with older/sicker populations (factor &gt; 1.0) will see lower adjusted per-enrollee spending, revealing the portion driven by demographics vs. price/utilization choices.<br/>
          <b>Fiscal impact:</b> (national avg − state rate) × estimated state claims. State claims estimated from national claims × state spending share.<br/>
          <b>Case mix:</b> Laspeyres decomposition. Price index holds utilization constant; mix index holds prices constant.<br/>
          <b>Concentration:</b> Gini coefficient and top-1%/5%/10% spending share by provider, per code.<br/>
          <b>Simulator:</b> FFS-adjusted figure uses approximate FFS/managed care splits by state (KFF/CMS 2023).
        </div></Card>
        <Card><CH t="Data Notes & Limitations"/><div style={{ padding:"4px 16px 12px",fontSize:11,color:A,lineHeight:1.8 }}>
          <b>T-MSIS ≠ fee schedule:</b> T-MSIS "avg paid per claim" reflects what was actually paid across all claim types, modifiers, and settings. It is not a fee schedule lookup. Expect Medicaid T-MSIS rates to be lower than fee schedule rates due to blended modifier/POS mix, MC encounters reported at $0 or capitated amounts, and claims processing reductions.<br/>
          <b>State assignment:</b> Provider state is derived from NPPES practice address. For telehealth or multistate organizations, claims may attribute to provider's registered state rather than where services were delivered.<br/>
          <b>Beneficiary counts:</b> The dataset reports TOTAL_UNIQUE_BENEFICIARIES at NPI × HCPCS × month — not deduplicated. Per-enrollee figures use published CMS enrollment milestones instead.<br/>
          <b>Suppression:</b> HHS suppresses cells with fewer than 11 beneficiaries. The dataset covers the majority of spending but excludes rare services.<br/>
          <b>Risk adjustment limitations:</b> Eligibility-mix adjustment accounts for broad demographic differences but does not capture within-group severity, chronic disease burden, or social determinants. Full risk adjustment requires beneficiary-level diagnosis data (e.g., CDPS scores) available only through restricted-use T-MSIS TAF files.<br/>
          <b>Data quality:</b> T-MSIS data quality varies by state. State Medicaid agencies remain the authoritative source for their own claims data.
        </div></Card>
        <div style={{ fontSize:10,color:AL }}>Aradune T-MSIS Explorer v0.7.5 · Built by <a href="https://aradune.co" style={{ color:cB,textDecoration:"none",fontWeight:600 }}>Aradune</a></div>
      </div>}

    </div>
  );
}
