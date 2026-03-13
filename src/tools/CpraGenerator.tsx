/**
 * CPRA Generator -- Comparative Payment Rate Analysis
 * 42 CFR §447.203 requires every state to publish a CPRA by July 1, 2026.
 * Uses Terminal B pre-computed data (cpra_em.json, dim_447_codes.json) as primary
 * source, with client-side fallback to medicaid_rates + medicare_rates.
 */
import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { STATE_NAMES } from "../data/states";
import { query as duckQuery } from "../lib/duckdb";
import { API_BASE } from "../lib/api";
import { useAradune } from "../context/AraduneContext";

// ── Design tokens ───────────────────────────────────────────────────────
const A = "#0A2540", AL = "#425A70", POS = "#2E6B4A", NEG = "#A4262C", WARN = "#B8860B";
const SF = "#F5F7F5", BD = "#E4EAE4", WH = "#fff", cB = "#2E6B4A";
const FM = "'SF Mono',Menlo,Consolas,monospace";
const FB = "'Helvetica Neue',Arial,sans-serif";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

// ── Category definitions (42 CFR 447.203 service categories) ────────────
const CATEGORY_META: Record<string, { label: string; description: string }> = {
  primary_care: { label: "Primary Care", description: "Office/outpatient E/M visits, preventive care, chronic care management" },
  obgyn: { label: "OB/GYN", description: "Maternity, delivery, and reproductive health services" },
  mh_sud: { label: "MH/SUD", description: "Outpatient mental health, psychiatry, psychotherapy, and substance use disorder services" },
};
const CATEGORY_IDS = Object.keys(CATEGORY_META);

// ── Types ───────────────────────────────────────────────────────────────
interface MedicareEntry { r: number; fr?: number; rvu?: number; w?: number; d?: string }
interface MedicareData { rates: Record<string, MedicareEntry>; cf: number; year: number }
type MedicaidRatesData = Record<string, Record<string, [number, string, string]>>;
interface ConvFactorEntry { name: string; methodology: string; methodology_detail: string; conversion_factors: unknown[]; cf_notes: string; update_frequency: string; gpci_approach: string; fee_schedule_type: string }
interface StateEntry { state: string; total_spend: number; total_claims: number; total_bene: number; n_providers: number; fmap: number; ffs_share?: number; [k: string]: unknown }

// Terminal B types
interface Dim447Entry { cpt_code: string; category: string; description: string; source: string }
interface CpraEmRow { procedure_code: string; medicaid_rate: number; medicare_nonfac_rate: number; medicare_fac_rate: number; pct_of_medicare: number; rate_description: string; em_category?: string }
type CpraEmData = Record<string, CpraEmRow[]>;
interface DqFlag { state_code: string; procedure_code: string; flag: string; detail: string }
interface DqEmData { summary: Record<string, number>; total_flags: number; state_rollups?: Record<string, Record<string, number>>; flags: DqFlag[] }
interface CpraSummaryState { code_count: number; avg_pct: number; median_pct: number; below_80_count: number; below_50_count: number; min_pct: number; max_pct: number; worst_category?: string; worst_category_pct?: number }
interface CpraSummaryData { national: { state_count: number; total_comparisons: number; avg_pct: number; median_pct: number }; states: Record<string, CpraSummaryState> }

interface CpraCodeRow {
  hcpcs: string;
  desc: string;
  category: string;
  categoryLabel: string;
  medicaidRate: number;
  rateSource: "fee_schedule" | "precomputed";
  medicareRate: number | null;
  pctMedicare: number | null;
  claims: number;
  bene: number;
  flag: "pass" | "warn" | "critical" | "na";
}

type SortKey = "hcpcs" | "desc" | "category" | "medicaidRate" | "medicareRate" | "pctMedicare" | "claims" | "bene";

// ── Helpers ─────────────────────────────────────────────────────────────
const STATES = Object.keys(STATE_NAMES).sort();
const fN = (n: number) => n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}K` : String(n);

// ── UI primitives ───────────────────────────────────────────────────────
const Card = ({ children, accent }: { children: React.ReactNode; accent?: string }) => (
  <div style={{ background: WH, borderRadius: 12, boxShadow: SH, padding: "20px 24px",
    borderTop: accent ? `3px solid ${accent}` : undefined, marginBottom: 20 }}>{children}</div>
);
const CH = ({ title, sub, right }: { title: string; sub?: string; right?: React.ReactNode }) => (
  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", marginBottom: 14, flexWrap: "wrap", gap: 8 }}>
    <div>
      <h3 style={{ margin: 0, fontSize: 15, fontWeight: 700, color: A, fontFamily: FB }}>{title}</h3>
      {sub && <p style={{ margin: "2px 0 0", fontSize: 12, color: AL }}>{sub}</p>}
    </div>
    {right}
  </div>
);
const Met = ({ label, value, color, sub }: { label: string; value: string; color?: string; sub?: string }) => (
  <div style={{ textAlign: "center", minWidth: 80 }}>
    <div style={{ fontSize: 22, fontWeight: 700, color: color || A, fontFamily: FM }}>{value}</div>
    <div style={{ fontSize: 11, color: AL, marginTop: 2 }}>{label}</div>
    {sub && <div style={{ fontSize: 10, color: AL, marginTop: 1 }}>{sub}</div>}
  </div>
);
const Pill = ({ label, active, onClick, color }: { label: string; active: boolean; onClick: () => void; color?: string }) => (
  <button onClick={onClick} style={{
    background: active ? `${color || cB}0D` : "transparent",
    border: active ? `1px solid ${color || cB}33` : `1px solid transparent`,
    borderRadius: 6, padding: "5px 14px", fontSize: 11, fontWeight: active ? 600 : 400,
    fontFamily: FB, color: active ? (color || cB) : AL, cursor: "pointer", transition: "all .15s",
  }}>{label}</button>
);
const Badge = ({ text, color }: { text: string; color: string }) => (
  <span style={{ display: "inline-block", fontSize: 9, fontWeight: 600, fontFamily: FM, padding: "1px 6px", borderRadius: 4, background: `${color}14`, color }}>{text}</span>
);

// ═════════════════════════════════════════════════════════════════════════
export default function CpraGenerator() {
  const { openIntelligence } = useAradune();
  const [st, setSt] = useState("FL");
  const [medicare, setMedicare] = useState<MedicareData | null>(null);
  const [medicaid, setMedicaid] = useState<MedicaidRatesData | null>(null);
  const [convFactors, setConvFactors] = useState<Record<string, ConvFactorEntry>>({});
  const [statesData, setStatesData] = useState<StateEntry[]>([]);
  const [dim447, setDim447] = useState<Dim447Entry[]>([]);
  const [cpraEm, setCpraEm] = useState<CpraEmData>({});
  const [dqFlags, setDqFlags] = useState<DqEmData | null>(null);
  const [dqStateNotes, setDqStateNotes] = useState<Record<string, { flags: string[]; notes: string[] }>>({});
  const [cpraSummary, setCpraSummary] = useState<CpraSummaryData | null>(null);
  const [claimsMap, setClaimsMap] = useState<Map<string, { paid: number; claims: number; bene: number }>>(new Map());
  const [loading, setLoading] = useState(true);
  const [claimsLoading, setClaimsLoading] = useState(false);
  const [tab, setTab] = useState("all");
  const [sortKey, setSortKey] = useState<SortKey>("pctMedicare");
  const [sortAsc, setSortAsc] = useState(true);
  const [exporting, setExporting] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // ── Mode: pre-computed comparison vs BYOD upload ──────────────────
  const [mode, setMode] = useState<"comparison" | "upload">("comparison");

  // ── Upload mode state ─────────────────────────────────────────────
  const [uploadSt, setUploadSt] = useState("FL");
  const [feeFile, setFeeFile] = useState<File | null>(null);
  const [utilFile, setUtilFile] = useState<File | null>(null);
  const [uploadLoading, setUploadLoading] = useState(false);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [uploadResult, setUploadResult] = useState<any>(null);
  const [uploadError, setUploadError] = useState<string | null>(null);
  const [uploadTab, setUploadTab] = useState("all");

  // ── Derived: code lookup from dim_447 ─────────────────────────────────
  const codeMap = useMemo(() => {
    const m = new Map<string, Dim447Entry>();
    for (const e of dim447) m.set(e.cpt_code, e);
    return m;
  }, [dim447]);
  const allCodes = useMemo(() => new Set(dim447.map(e => e.cpt_code)), [dim447]);

  // ── Reference data load (one-time) ──────────────────────────────────
  useEffect(() => {
    Promise.all([
      fetch("/data/medicare_rates.json").then(r => r.json()),
      fetch("/data/medicaid_rates.json").then(r => r.json()),
      fetch("/data/conversion_factors.json").then(r => r.json()),
      fetch("/data/states.json").then(r => r.json()),
      fetch("/data/dim_447_codes.json").then(r => r.json()),
      fetch("/data/cpra_summary.json").then(r => r.json()).catch(() => null),
      fetch("/data/dq_state_notes.json").then(r => r.ok ? r.json() : {}).catch(() => ({})),
    ]).then(([mc, md, cf, sts, dim, summ, dqNotes]) => {
      // Normalize medicare data: static file is {code: {rates: {state: {r,fr}}, d, w}}
      // but MedicareData expects {rates: {code: {r,fr,d}}, cf, year}
      if (mc && !mc.rates && !mc.cf) {
        // Static file shape — wrap it
        setMedicare({ rates: mc, cf: 33.4009, year: 2025 } as MedicareData);
      } else {
        setMedicare(mc);
      }
      setMedicaid(md);
      setConvFactors(cf);
      setStatesData(sts);
      setDim447(Array.isArray(dim) ? dim : []);
      if (summ) setCpraSummary(summ);
      if (dqNotes) setDqStateNotes(dqNotes);
      setLoading(false);
    }).catch(err => {
      console.error("CPRA data load failed:", err);
      setError(`Failed to load data: ${err?.message || String(err)}`);
      setLoading(false);
    });
  }, []);

  // ── Per-state CPRA rates from API (with static fallback) ──────────
  const cpraEmCache = useRef<CpraEmData>({});
  const dqCache = useRef<Record<string, DqEmData | null>>({});
  const dqBulkLoaded = useRef(false);

  useEffect(() => {
    if (!st) return;
    // Already cached
    if (cpraEmCache.current[st]) {
      setCpraEm(cpraEmCache.current);
      return;
    }

    // Try API first, then fall back to bulk static JSON
    const loadRates = async () => {
      try {
        const res = await fetch(`${API_BASE}/api/cpra/rates/${st}?em_only=true`);
        if (res.ok) {
          const rows: Array<{ procedure_code: string; medicaid_rate: number; medicare_nonfac_rate: number; medicare_fac_rate: number; pct_of_medicare: number; description: string; em_category?: string }> = await res.json();
          // Map API shape to frontend CpraEmRow shape
          const mapped: CpraEmRow[] = rows.map(r => ({
            procedure_code: r.procedure_code,
            medicaid_rate: r.medicaid_rate,
            medicare_nonfac_rate: r.medicare_nonfac_rate,
            medicare_fac_rate: r.medicare_fac_rate,
            pct_of_medicare: r.pct_of_medicare,
            rate_description: r.description || "",
            em_category: r.em_category,
          }));
          cpraEmCache.current = { ...cpraEmCache.current, [st]: mapped };
          setCpraEm({ ...cpraEmCache.current });
          return;
        }
      } catch { /* API unavailable, fall through */ }

      // Fallback: load bulk static file
      if (Object.keys(cpraEmCache.current).length === 0) {
        try {
          const em = await fetch("/data/cpra_em.json").then(r => r.json());
          if (em && typeof em === "object") {
            cpraEmCache.current = em;
            setCpraEm(em);
            return;
          }
        } catch { /* no static data either */ }
      }
    };

    const loadDq = async () => {
      if (dqCache.current[st] !== undefined) {
        if (dqCache.current[st]) setDqFlags(dqCache.current[st]);
        return;
      }
      try {
        const res = await fetch(`${API_BASE}/api/cpra/dq/${st}`);
        if (res.ok) {
          const flags: DqFlag[] = await res.json();
          const summary: Record<string, number> = {};
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const mapped: DqFlag[] = (flags as any[]).map((f: any) => ({
            state_code: f.state_code || st,
            procedure_code: f.entity_id || f.procedure_code || "",
            flag: f.flag_type || f.flag || "",
            detail: f.detail || "",
          }));
          for (const f of mapped) summary[f.flag] = (summary[f.flag] || 0) + 1;
          const dqData: DqEmData = { summary, total_flags: mapped.length, flags: mapped };
          dqCache.current[st] = dqData;
          setDqFlags(dqData);
          return;
        }
      } catch { /* fall through */ }

      // Fallback: bulk static
      if (!dqBulkLoaded.current) {
        try {
          const dq = await fetch("/data/dq_flags_em.json").then(r => r.json());
          if (dq) {
            dqBulkLoaded.current = true;
            setDqFlags(dq);
          }
        } catch { /* no data */ }
      }
    };

    loadRates();
    loadDq();
  }, [st]);

  // ── Claims query per state ──────────────────────────────────────────
  useEffect(() => {
    if (!st || allCodes.size === 0) return;
    setClaimsLoading(true);
    const codeList = [...allCodes].map(c => `'${c}'`).join(",");
    duckQuery(`
      SELECT hcpcs_code, SUM(total_paid) as total_paid, SUM(total_claims) as total_claims, SUM(total_bene) as total_bene
      FROM claims
      WHERE state = '${st}' AND year = 2023 AND hcpcs_code IN (${codeList})
      GROUP BY hcpcs_code
    `).then(res => {
      const m = new Map<string, { paid: number; claims: number; bene: number }>();
      for (const r of res.rows) {
        m.set(r.hcpcs_code as string, {
          paid: Number(r.total_paid) || 0,
          claims: Number(r.total_claims) || 0,
          bene: Number(r.total_bene) || 0,
        });
      }
      setClaimsMap(m);
      setClaimsLoading(false);
    }).catch(() => {
      setClaimsMap(new Map());
      setClaimsLoading(false);
    });
  }, [st, allCodes]);

  // ── Build code rows ─────────────────────────────────────────────────
  const rows: CpraCodeRow[] = useMemo(() => {
    if (!medicare?.rates || !medicaid || dim447.length === 0) return [];

    // Pre-computed rows from Terminal B (primary source)
    const emRows = cpraEm[st] || [];
    const emMap = new Map<string, CpraEmRow>();
    for (const r of emRows) emMap.set(r.procedure_code, r);

    const stMedicaid = medicaid[st] || {};
    const out: CpraCodeRow[] = [];

    for (const code of allCodes) {
      const dimEntry = codeMap.get(code)!;
      const catId = dimEntry.category;
      const catMeta = CATEGORY_META[catId];
      if (!catMeta) continue;
      const claim = claimsMap.get(code);

      // Check Terminal B pre-computed data first
      const emRow = emMap.get(code);
      if (emRow) {
        // Terminal B provides locality-adjusted Medicaid vs Medicare
        const pctMedicare = emRow.pct_of_medicare;
        const flag: CpraCodeRow["flag"] = pctMedicare < 50 ? "critical" : pctMedicare < 80 ? "warn" : "pass";

        out.push({
          hcpcs: code,
          desc: emRow.rate_description || dimEntry.description,
          category: catId,
          categoryLabel: catMeta.label,
          medicaidRate: emRow.medicaid_rate,
          rateSource: "fee_schedule",
          medicareRate: emRow.medicare_nonfac_rate,
          pctMedicare,
          claims: claim?.claims || 0,
          bene: claim?.bene || 0,
          flag,
        });
        continue;
      }

      // Fallback: client-side rate resolution
      let medicaidRate = 0;
      let rateSource: "fee_schedule" | "precomputed" = "precomputed";

      const fsEntry = stMedicaid[code];
      if (fsEntry && fsEntry[0] > 0) {
        medicaidRate = fsEntry[0];
        rateSource = "fee_schedule";
      } else if (claim && claim.claims > 0) {
        medicaidRate = claim.paid / claim.claims;
        rateSource = "precomputed";
      }

      if (medicaidRate <= 0) continue;

      const mcRaw = medicare.rates[code] as MedicareEntry | any;
      // Handle both API shape ({r, fr, d}) and static file shape ({rates: {state: {r, fr}}, d, w})
      let medicareRate: number | null = null;
      let mcDesc: string | undefined;
      if (mcRaw) {
        if (typeof mcRaw.r === "number") {
          // API wrapper shape: entry is {r, fr, d, w}
          medicareRate = mcRaw.r;
          mcDesc = mcRaw.d;
        } else if (mcRaw.rates && mcRaw.rates[st]) {
          // Static file shape: entry is {rates: {state: {r, fr}}, d, w}
          medicareRate = mcRaw.rates[st]?.r ?? null;
          mcDesc = mcRaw.d;
        }
      }
      const desc = (fsEntry && fsEntry[1]) || dimEntry.description || mcDesc || code;

      const pctMedicare = (medicareRate && medicareRate > 0) ? (medicaidRate / medicareRate) * 100 : null;
      let flag: CpraCodeRow["flag"] = "na";
      if (pctMedicare !== null) {
        flag = pctMedicare < 50 ? "critical" : pctMedicare < 80 ? "warn" : "pass";
      }

      out.push({
        hcpcs: code, desc,
        category: catId, categoryLabel: catMeta.label,
        medicaidRate, rateSource, medicareRate, pctMedicare,
        claims: claim?.claims || 0, bene: claim?.bene || 0, flag,
      });
    }

    return out;
  }, [st, medicare, medicaid, cpraEm, claimsMap, dim447, codeMap, allCodes]);

  // ── Filtered & sorted rows ──────────────────────────────────────────
  const filtered = useMemo(() => {
    const f = tab === "all" ? rows : rows.filter(r => r.category === tab);
    return [...f].sort((a, b) => {
      let av: number | string = 0, bv: number | string = 0;
      if (sortKey === "hcpcs") { av = a.hcpcs; bv = b.hcpcs; }
      else if (sortKey === "desc") { av = a.desc; bv = b.desc; }
      else if (sortKey === "category") { av = a.categoryLabel; bv = b.categoryLabel; }
      else if (sortKey === "medicaidRate") { av = a.medicaidRate; bv = b.medicaidRate; }
      else if (sortKey === "medicareRate") { av = a.medicareRate ?? -1; bv = b.medicareRate ?? -1; }
      else if (sortKey === "pctMedicare") { av = a.pctMedicare ?? -1; bv = b.pctMedicare ?? -1; }
      else if (sortKey === "claims") { av = a.claims; bv = b.claims; }
      else if (sortKey === "bene") { av = a.bene; bv = b.bene; }
      if (typeof av === "string") return sortAsc ? av.localeCompare(bv as string) : (bv as string).localeCompare(av);
      return sortAsc ? (av as number) - (bv as number) : (bv as number) - (av as number);
    });
  }, [rows, tab, sortKey, sortAsc]);

  // ── Aggregate stats ─────────────────────────────────────────────────
  const stats = useMemo(() => {
    const parityCodes = rows.filter(r => r.pctMedicare !== null);
    const totalCodes = rows.length;
    const below80 = parityCodes.filter(r => r.pctMedicare! < 80).length;
    const below50 = parityCodes.filter(r => r.pctMedicare! < 50).length;
    const totalClaims = rows.reduce((s, r) => s + r.claims, 0);
    const totalBene = rows.reduce((s, r) => s + r.bene, 0);

    let weightedSum = 0, weightTotal = 0;
    for (const r of parityCodes) {
      const w = Math.max(r.claims, 1);
      weightedSum += r.pctMedicare! * w;
      weightTotal += w;
    }
    const weightedAvg = weightTotal > 0 ? weightedSum / weightTotal : 0;

    const catScores = CATEGORY_IDS.map(catId => {
      const catRows = parityCodes.filter(r => r.category === catId);
      if (catRows.length === 0) return { id: catId, avg: 100 };
      let ws = 0, wt = 0;
      for (const r of catRows) { const w = Math.max(r.claims, 1); ws += r.pctMedicare! * w; wt += w; }
      return { id: catId, avg: wt > 0 ? ws / wt : 100 };
    });
    const worstCat = catScores.reduce((a, b) => a.avg < b.avg ? a : b, catScores[0]);
    const overallStatus = worstCat.avg < 50 ? "critical" : worstCat.avg < 80 ? "warn" : "pass";

    const catSummary = CATEGORY_IDS.map(catId => {
      const catRows = rows.filter(r => r.category === catId);
      const catParity = catRows.filter(r => r.pctMedicare !== null);
      let ws = 0, wt = 0;
      for (const r of catParity) { const w = Math.max(r.claims, 1); ws += r.pctMedicare! * w; wt += w; }
      return {
        id: catId,
        label: CATEGORY_META[catId].label,
        totalCodes: catRows.length,
        parityCodes: catParity.length,
        weightedAvg: wt > 0 ? ws / wt : 0,
        below80: catParity.filter(r => r.pctMedicare! < 80).length,
        below50: catParity.filter(r => r.pctMedicare! < 50).length,
        totalClaims: catRows.reduce((s, r) => s + r.claims, 0),
        totalBene: catRows.reduce((s, r) => s + r.bene, 0),
      };
    });

    const fsCodes = rows.filter(r => r.rateSource === "fee_schedule").length;
    const precomputedCodes = rows.filter(r => r.rateSource === "precomputed").length;

    return { totalCodes, below80, below50, totalClaims, totalBene, weightedAvg, overallStatus, catSummary, fsCodes, precomputedCodes };
  }, [rows]);

  // ── State metadata ──────────────────────────────────────────────────
  const stateInfo = useMemo(() => statesData.find(s => s.state === st), [statesData, st]);
  const stateConv = useMemo(() => convFactors[st], [convFactors, st]);
  const ffsShare = stateInfo?.ffs_share ?? null;

  // ── Pipeline summary for current state ─────────────────────────────
  const pipeSummary = useMemo(() => cpraSummary?.states?.[st] ?? null, [cpraSummary, st]);
  const national = cpraSummary?.national ?? null;

  // ── DQ flags for current state ──────────────────────────────────────
  const stateDqFlags = useMemo(() => {
    if (!dqFlags) return [];
    return dqFlags.flags.filter(f => f.state_code === st);
  }, [dqFlags, st]);
  const dqSummary = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const f of stateDqFlags) counts[f.flag] = (counts[f.flag] || 0) + 1;
    return counts;
  }, [stateDqFlags]);

  // ── Sort handler ────────────────────────────────────────────────────
  const handleSort = useCallback((key: SortKey) => {
    if (key === sortKey) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(key === "pctMedicare"); }
  }, [sortKey, sortAsc]);

  // ── Export handlers ─────────────────────────────────────────────────
  const handlePdfExport = useCallback(async () => {
    setExporting("pdf");
    try {
      const { generateCpraPdf } = await import("../utils/cpraPdf");
      await generateCpraPdf({
        state: st, stateName: STATE_NAMES[st] || st,
        rows, stats, catSummary: stats.catSummary, stateConv, ffsShare,
      });
    } finally { setExporting(null); }
  }, [st, rows, stats, stateConv, ffsShare]);

  const handleXlsxExport = useCallback(async () => {
    setExporting("xlsx");
    try {
      const { generateCpraXlsx } = await import("../utils/cpraXlsx");
      await generateCpraXlsx({
        state: st, stateName: STATE_NAMES[st] || st,
        rows, stats, catSummary: stats.catSummary, stateConv, ffsShare,
      });
    } finally { setExporting(null); }
  }, [st, rows, stats, stateConv, ffsShare]);

  const handleHtmlExport = useCallback(() => {
    setExporting("html");
    try {
      const stateName = STATE_NAMES[st] || st;
      const date = new Date().toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" });

      const catHtml = CATEGORY_IDS.map(catId => {
        const cat = CATEGORY_META[catId];
        const catRows = rows.filter(r => r.category === catId);
        if (catRows.length === 0) return "";
        const sorted = [...catRows].sort((a, b) => (a.pctMedicare ?? 999) - (b.pctMedicare ?? 999));
        const tableRows = sorted.map(r => {
          const rowColor = r.flag === "critical" ? "#FDF2F2" : r.flag === "warn" ? "#FFFBEB" : "#fff";
          const pctColor = r.flag === "critical" ? NEG : r.flag === "warn" ? WARN : A;
          return `<tr style="background:${rowColor}">
            <td style="padding:4px 8px;font-family:monospace;font-size:12px">${r.hcpcs}</td>
            <td style="padding:4px 8px;font-size:12px">${r.desc}</td>
            <td style="padding:4px 8px;text-align:right;font-size:12px">$${r.medicaidRate.toFixed(2)}</td>
            <td style="padding:4px 8px;text-align:right;font-size:12px">${r.medicareRate !== null ? "$" + r.medicareRate.toFixed(2) : "N/A"}</td>
            <td style="padding:4px 8px;text-align:right;font-size:12px;color:${pctColor};font-weight:600">${r.pctMedicare !== null ? r.pctMedicare.toFixed(1) + "%" : "N/A"}</td>
            <td style="padding:4px 8px;text-align:right;font-size:12px">${r.claims > 0 ? fN(r.claims) : "-"}</td>
          </tr>`;
        }).join("\n");

        return `<h3 style="margin:24px 0 8px;color:#2E6B4A;font-family:Helvetica Neue,Arial,sans-serif">${cat.label}</h3>
        <p style="margin:0 0 12px;color:#425A70;font-size:12px">${cat.description}</p>
        <table style="width:100%;border-collapse:collapse;border:1px solid #E4EAE4">
          <thead><tr style="background:#2E6B4A;color:#fff">
            <th style="padding:6px 8px;text-align:left;font-size:11px">HCPCS</th>
            <th style="padding:6px 8px;text-align:left;font-size:11px">Description</th>
            <th style="padding:6px 8px;text-align:right;font-size:11px">Medicaid</th>
            <th style="padding:6px 8px;text-align:right;font-size:11px">Medicare</th>
            <th style="padding:6px 8px;text-align:right;font-size:11px">% MCR</th>
            <th style="padding:6px 8px;text-align:right;font-size:11px">Claims</th>
          </tr></thead>
          <tbody>${tableRows}</tbody>
        </table>`;
      }).join("\n");

      const html = `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CPRA -- ${stateName} -- ${date}</title>
<style>
  body { font-family: 'Helvetica Neue', Arial, sans-serif; max-width: 1000px; margin: 0 auto; padding: 24px; color: #0A2540; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  h2 { font-size: 16px; margin: 24px 0 8px; color: #0A2540; }
  .meta { color: #425A70; font-size: 12px; margin: 0 0 20px; }
  .summary { display: flex; gap: 24px; flex-wrap: wrap; margin: 16px 0; }
  .stat { text-align: center; }
  .stat-val { font-size: 20px; font-weight: 700; font-family: 'SF Mono', Menlo, monospace; }
  .stat-label { font-size: 11px; color: #425A70; margin-top: 2px; }
  .notes { background: #F5F7F5; border-radius: 8px; padding: 16px; margin: 24px 0; font-size: 12px; color: #425A70; line-height: 1.6; }
  table { font-family: 'Helvetica Neue', Arial, sans-serif; }
</style></head><body>
<h1>Comparative Payment Rate Analysis</h1>
<p class="meta">${stateName} (${st}) | Generated ${date} | 42 CFR &sect;447.203</p>

<h2>Summary</h2>
<div class="summary">
  <div class="stat"><div class="stat-val">${stats.totalCodes}</div><div class="stat-label">Codes analyzed</div></div>
  <div class="stat"><div class="stat-val" style="color:${stats.weightedAvg < 80 ? NEG : POS}">${stats.weightedAvg.toFixed(1)}%</div><div class="stat-label">Wtd avg % MCR</div></div>
  <div class="stat"><div class="stat-val" style="color:${WARN}">${stats.below80}</div><div class="stat-label">Below 80%</div></div>
  <div class="stat"><div class="stat-val" style="color:${NEG}">${stats.below50}</div><div class="stat-label">Below 50%</div></div>
  <div class="stat"><div class="stat-val">${fN(stats.totalClaims)}</div><div class="stat-label">Claims (2023)</div></div>
</div>

${catHtml}

<div class="notes">
<strong>Data Notes</strong><br>
Service year: 2023 T-MSIS | FFS claims only (~${ffsShare !== null && ffsShare !== undefined && !isNaN(Number(ffsShare)) ? (Number(ffsShare) * 100).toFixed(0) : "N/A"}% of Medicaid in ${st} is FFS)<br>
Rate sources: ${stats.fsCodes} codes from fee schedule, ${stats.precomputedCodes} codes from T-MSIS<br>
Medicare benchmark: CY2026 PFS locality-adjusted rates (GPCI-weighted by state)<br>
Beneficiary counts represent patient-service events, not unique individuals<br>
<br>
<strong>Regulatory Reference</strong><br>
42 CFR &sect;447.203 | CMS Ensuring Access Final Rule | Compliance deadline: July 1, 2026<br>
${stateConv ? `State methodology: ${stateConv.methodology_detail || stateConv.methodology}` : ""}
</div>
<p style="font-size:10px;color:#999;margin-top:24px">Generated by Aradune | aradune.co</p>
</body></html>`;

      const a = document.createElement("a");
      const url = URL.createObjectURL(new Blob([html], { type: "text/html" }));
      a.href = url;
      a.download = `cpra_${st}_${new Date().toISOString().split("T")[0]}.html`;
      a.click();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } finally { setExporting(null); }
  }, [st, rows, stats, stateConv, ffsShare]);

  // ── Threshold bar data ──────────────────────────────────────────────
  const barData = useMemo(() => {
    return rows
      .filter(r => r.pctMedicare !== null)
      .sort((a, b) => a.pctMedicare! - b.pctMedicare!)
      .map(r => ({ code: r.hcpcs, pct: r.pctMedicare!, flag: r.flag }));
  }, [rows]);

  // ── Upload handler ─────────────────────────────────────────────────
  const handleUploadGenerate = useCallback(async () => {
    if (!feeFile || !utilFile) return;
    setUploadLoading(true);
    setUploadError(null);
    setUploadResult(null);

    const form = new FormData();
    form.append("state", uploadSt);
    form.append("fee_schedule", feeFile);
    form.append("utilization", utilFile);

    try {
      const res = await fetch(`${API_BASE}/api/cpra/upload/generate`, {
        method: "POST",
        body: form,
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(
          typeof err.detail === "string" ? err.detail
            : err.detail?.errors?.[0]?.message
            || err.detail?.message
            || JSON.stringify(err.detail || err)
        );
      }
      const data = await res.json();
      setUploadResult(data);
    } catch (e: unknown) {
      setUploadError(e instanceof Error ? e.message : String(e));
    } finally {
      setUploadLoading(false);
    }
  }, [uploadSt, feeFile, utilFile]);

  const handleUploadReport = useCallback(async () => {
    if (!feeFile || !utilFile) return;
    setUploadLoading(true);

    const form = new FormData();
    form.append("state", uploadSt);
    form.append("fee_schedule", feeFile);
    form.append("utilization", utilFile);

    try {
      const res = await fetch(`${API_BASE}/api/cpra/upload/generate/report`, {
        method: "POST",
        body: form,
      });
      if (!res.ok) throw new Error("Report generation failed");
      const blob = await res.blob();
      const a = document.createElement("a");
      a.href = URL.createObjectURL(blob);
      a.download = `cpra_${uploadSt.toLowerCase()}_report.html`;
      a.click();
      setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    } catch (e: unknown) {
      setUploadError(e instanceof Error ? e.message : String(e));
    } finally {
      setUploadLoading(false);
    }
  }, [uploadSt, feeFile, utilFile]);

  // ── Loading / error state ───────────────────────────────────────────
  if (loading) {
    return (
      <div style={{ maxWidth: 1080, margin: "0 auto", padding: "80px 20px", textAlign: "center" }}>
        <div style={{ fontSize: 13, color: AL, fontFamily: FB }}>Loading CPRA data...</div>
      </div>
    );
  }
  if (error) {
    return (
      <div style={{ maxWidth: 1080, margin: "0 auto", padding: "80px 20px", textAlign: "center" }}>
        <div style={{ fontSize: 14, color: NEG, fontFamily: FB, marginBottom: 8 }}>Error loading CPRA data</div>
        <div style={{ fontSize: 12, color: AL, fontFamily: FM }}>{error}</div>
      </div>
    );
  }

  // ── Render ──────────────────────────────────────────────────────────
  const statusColor = stats.overallStatus === "critical" ? NEG : stats.overallStatus === "warn" ? WARN : POS;
  const statusLabel = stats.overallStatus === "critical" ? "Critical gaps identified" : stats.overallStatus === "warn" ? "Below parity in key categories" : "Meeting parity benchmarks";

  return (
    <div style={{ maxWidth: 1080, margin: "0 auto", padding: "0 20px 48px", fontFamily: FB }}>
      {/* Header */}
      <div style={{ padding: "28px 0 12px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", flexWrap: "wrap", gap: 12 }}>
          <div>
            <h2 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: A, letterSpacing: -0.3 }}>
              Comparative Payment Rate Analysis
            </h2>
            <p style={{ margin: "4px 0 0", fontSize: 12, color: AL }}>
              42 CFR &sect;447.203 | CMS Ensuring Access Final Rule | Deadline: July 1, 2026
            </p>
          </div>
          <button onClick={() => openIntelligence({ state: st, summary: `User is viewing CPRA Compliance for ${STATE_NAMES[st] || st}` })} style={{
            padding: "8px 14px", borderRadius: 8, border: "none",
            background: "#2E6B4A", color: "#fff", fontSize: 12, cursor: "pointer", fontFamily: FB, fontWeight: 600,
          }}>Ask Aradune</button>
        </div>
        {/* Mode toggle */}
        <div style={{ display: "flex", gap: 4, marginTop: 16 }}>
          <Pill label="Cross-State Comparison" active={mode === "comparison"} onClick={() => setMode("comparison")} />
          <Pill label="Bring Your Own Data" active={mode === "upload"} onClick={() => setMode("upload")} color="#C4590A" />
        </div>
      </div>

      {/* ═══ UPLOAD MODE ═══ */}
      {mode === "upload" ? (
        <>
          <Card accent="#C4590A">
            <CH title="Upload Fee Schedule & Utilization Data" sub="Generate a 42 CFR 447.203 compliant CPRA from your own data. Uses 68 CMS CY2025 E/M codes, $32.3465 CF, per-locality Medicare rates." />
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginTop: 12 }}>
              <div>
                <label style={{ display: "block", fontSize: 11, fontWeight: 600, color: A, marginBottom: 4 }}>State</label>
                <select value={uploadSt} onChange={e => setUploadSt(e.target.value)} style={{
                  width: "100%", padding: "7px 10px", borderRadius: 6, border: `1px solid ${BD}`,
                  fontSize: 12, fontFamily: FB, color: A, background: WH,
                }}>
                  {STATES.map(s => <option key={s} value={s}>{s} -- {STATE_NAMES[s]}</option>)}
                </select>
              </div>
              <div />
              <div>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                  <label style={{ fontSize: 11, fontWeight: 600, color: A }}>Fee Schedule CSV</label>
                  <a href={`${API_BASE}/api/cpra/upload/templates/fee-schedule`} style={{ fontSize: 10, color: cB }}>Download template</a>
                </div>
                <input type="file" accept=".csv" onChange={e => setFeeFile(e.target.files?.[0] || null)} style={{
                  width: "100%", padding: "6px", borderRadius: 6, border: `1px solid ${BD}`,
                  fontSize: 11, fontFamily: FB, background: SF,
                }} />
                <div style={{ fontSize: 10, color: AL, marginTop: 2 }}>Columns: hcpcs_code, medicaid_rate</div>
              </div>
              <div>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                  <label style={{ fontSize: 11, fontWeight: 600, color: A }}>Utilization CSV</label>
                  <a href={`${API_BASE}/api/cpra/upload/templates/utilization`} style={{ fontSize: 10, color: cB }}>Download template</a>
                </div>
                <input type="file" accept=".csv" onChange={e => setUtilFile(e.target.files?.[0] || null)} style={{
                  width: "100%", padding: "6px", borderRadius: 6, border: `1px solid ${BD}`,
                  fontSize: 11, fontFamily: FB, background: SF,
                }} />
                <div style={{ fontSize: 10, color: AL, marginTop: 2 }}>Columns: hcpcs_code, category, total_claims, unique_beneficiaries</div>
              </div>
            </div>
            <div style={{ display: "flex", gap: 8, marginTop: 16 }}>
              <button
                onClick={handleUploadGenerate}
                disabled={!feeFile || !utilFile || uploadLoading}
                style={{
                  padding: "8px 20px", borderRadius: 6, border: "none",
                  background: feeFile && utilFile ? "#C4590A" : BD, color: WH,
                  fontSize: 12, fontWeight: 600, cursor: feeFile && utilFile ? "pointer" : "default",
                  opacity: uploadLoading ? 0.6 : 1,
                }}
              >
                {uploadLoading ? "Generating..." : "Generate CPRA"}
              </button>
              {uploadResult && (
                <button
                  onClick={handleUploadReport}
                  disabled={uploadLoading}
                  style={{
                    padding: "8px 20px", borderRadius: 6, border: `1px solid ${BD}`,
                    background: WH, color: A, fontSize: 12, fontWeight: 600, cursor: "pointer",
                  }}
                >
                  Download HTML Report
                </button>
              )}
            </div>
            {uploadError && (
              <div style={{ marginTop: 12, padding: "8px 12px", background: "#FEE2E2", borderRadius: 6, fontSize: 12, color: NEG }}>{uploadError}</div>
            )}
          </Card>

          {/* Upload results */}
          {uploadResult && (() => {
            const m = uploadResult.meta;
            const cats: Array<{ category: string; weighted_pct_medicare: number; median_pct_medicare: number; min_pct_medicare: number; max_pct_medicare: number; n_codes: number; total_claims: number }> = uploadResult.category_summary || [];
            const statewide: Array<{ hcpcs_code: string; description: string; category: string; medicaid_rate: number; has_medicaid_rate: boolean; medicare_nf_rate_avg: number; pct_of_medicare_avg: number | null; total_claims: number; unique_beneficiaries: number; is_suppressed: boolean }> = uploadResult.statewide || [];
            const noRate: Array<{ hcpcs_code: string; description: string; medicare_nf_rate_avg: number }> = uploadResult.codes_no_rate || [];
            const uploadCatIds = ["Primary Care", "OB-GYN", "Outpatient MH/SUD"];
            const filteredUpload = uploadTab === "all" ? statewide : statewide.filter(r => r.category === uploadTab);

            return (
              <>
                {/* Summary card */}
                <Card accent={cB}>
                  <CH title={`CPRA Results: ${m.state_name}`} sub={`${m.n_codes} E/M codes | ${m.n_with_rate} with rates | ${m.n_without_rate} missing | CF=$${m.conversion_factor}`} />
                  <div style={{ display: "flex", gap: 16, flexWrap: "wrap", justifyContent: "space-around", padding: "8px 0" }}>
                    {cats.map(c => (
                      <Met key={c.category} label={c.category}
                        value={c.weighted_pct_medicare != null ? `${c.weighted_pct_medicare}%` : "---"}
                        color={c.weighted_pct_medicare < 80 ? NEG : POS}
                        sub={`${c.n_codes} codes, ${c.total_claims.toLocaleString()} claims`}
                      />
                    ))}
                  </div>
                  {m.warnings?.length > 0 && (
                    <div style={{ marginTop: 12, padding: "8px 12px", background: "#FFFBEB", borderRadius: 6, fontSize: 11, color: WARN }}>
                      {m.warnings.map((w: string, i: number) => <div key={i}>{w}</div>)}
                    </div>
                  )}
                </Card>

                {/* Category filter tabs */}
                <div style={{ display: "flex", gap: 4, marginBottom: 16, flexWrap: "wrap" }}>
                  <Pill label="All" active={uploadTab === "all"} onClick={() => setUploadTab("all")} />
                  {uploadCatIds.map(cat => (
                    <Pill key={cat} label={cat} active={uploadTab === cat} onClick={() => setUploadTab(cat)} />
                  ))}
                </div>

                {/* Code-level table */}
                <Card>
                  <CH title="Code-Level Comparison" sub={`${filteredUpload.length} code-category pairs`} />
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                      <thead>
                        <tr style={{ borderBottom: `2px solid ${BD}` }}>
                          {["Code", "Description", "Category", "Medicaid", "Medicare (Avg)", "% MCR", "Claims", "Bene"].map(h => (
                            <th key={h} style={{ padding: "8px 6px", textAlign: h === "Code" || h === "Description" || h === "Category" ? "left" : "right",
                              fontSize: 10, fontWeight: 600, color: AL, fontFamily: FM, letterSpacing: 0.3 }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {filteredUpload.map((r, ri) => {
                          const pct = r.pct_of_medicare_avg;
                          const flag = !r.has_medicaid_rate ? "na" : pct == null ? "na" : pct < 50 ? "critical" : pct < 80 ? "warn" : "pass";
                          return (
                            <tr key={`${r.hcpcs_code}-${r.category}-${ri}`} style={{ background: ri % 2 === 0 ? "transparent" : `${SF}60`, borderBottom: `1px solid ${BD}40` }}>
                              <td style={{ padding: "6px", fontFamily: FM, fontSize: 11 }}>{r.hcpcs_code}</td>
                              <td style={{ padding: "6px", maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.description}</td>
                              <td style={{ padding: "6px" }}>
                                <Badge text={r.category} color={r.category === "Primary Care" ? cB : r.category === "OB-GYN" ? "#7B3FA0" : "#2563EB"} />
                              </td>
                              <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11 }}>
                                {r.has_medicaid_rate ? `$${r.medicaid_rate.toFixed(2)}` : "---"}
                              </td>
                              <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11 }}>${r.medicare_nf_rate_avg.toFixed(2)}</td>
                              <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11, fontWeight: 600,
                                color: flag === "critical" ? NEG : flag === "warn" ? WARN : flag === "pass" ? POS : AL }}>
                                {pct != null ? `${pct}%` : "---"}
                              </td>
                              <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11, color: AL }}>
                                {r.is_suppressed ? "*" : r.total_claims > 0 ? r.total_claims.toLocaleString() : "-"}
                              </td>
                              <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11, color: AL }}>
                                {r.is_suppressed ? "*" : r.unique_beneficiaries > 0 ? r.unique_beneficiaries.toLocaleString() : "-"}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </Card>

                {/* Missing codes */}
                {noRate.length > 0 && (
                  <Card>
                    <CH title={`${noRate.length} E/M Codes Without Medicaid Rate`} sub="These codes do not appear on the uploaded fee schedule" />
                    <div style={{ overflowX: "auto" }}>
                      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                        <thead>
                          <tr style={{ borderBottom: `2px solid ${BD}` }}>
                            <th style={{ padding: "8px 6px", textAlign: "left", fontSize: 10, fontWeight: 600, color: AL, fontFamily: FM }}>Code</th>
                            <th style={{ padding: "8px 6px", textAlign: "left", fontSize: 10, fontWeight: 600, color: AL, fontFamily: FM }}>Description</th>
                            <th style={{ padding: "8px 6px", textAlign: "right", fontSize: 10, fontWeight: 600, color: AL, fontFamily: FM }}>Medicare Rate (Avg)</th>
                          </tr>
                        </thead>
                        <tbody>
                          {noRate.map((r, i) => (
                            <tr key={r.hcpcs_code} style={{ background: i % 2 === 0 ? "transparent" : `${SF}60`, borderBottom: `1px solid ${BD}40` }}>
                              <td style={{ padding: "6px", fontFamily: FM, fontSize: 11 }}>{r.hcpcs_code}</td>
                              <td style={{ padding: "6px" }}>{r.description}</td>
                              <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11 }}>${r.medicare_nf_rate_avg.toFixed(2)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </Card>
                )}
              </>
            );
          })()}
        </>
      ) : (
      <>
      {/* ═══ COMPARISON MODE (original UI) ═══ */}
      <div style={{ display: "flex", justifyContent: "flex-end", gap: 8, alignItems: "center", flexWrap: "wrap", marginBottom: 16 }}>
        <select value={st} onChange={e => setSt(e.target.value)} style={{
          padding: "7px 10px", borderRadius: 6, border: `1px solid ${BD}`, fontSize: 12,
          fontFamily: FB, color: A, background: WH, minWidth: 180,
        }}>
          {STATES.map(s => <option key={s} value={s}>{s} -- {STATE_NAMES[s]}</option>)}
        </select>
        <button onClick={handlePdfExport} disabled={!!exporting} style={{
          padding: "7px 14px", borderRadius: 6, border: `1px solid ${BD}`,
          background: WH, fontSize: 11, fontWeight: 600, cursor: "pointer", color: A,
        }}>{exporting === "pdf" ? "..." : "PDF"}</button>
        <button onClick={handleXlsxExport} disabled={!!exporting} style={{
          padding: "7px 14px", borderRadius: 6, border: `1px solid ${BD}`,
          background: WH, fontSize: 11, fontWeight: 600, cursor: "pointer", color: A,
        }}>{exporting === "xlsx" ? "..." : "Excel"}</button>
        <button onClick={handleHtmlExport} disabled={!!exporting} style={{
          padding: "7px 14px", borderRadius: 6, border: `1px solid ${BD}`,
          background: WH, fontSize: 11, fontWeight: 600, cursor: "pointer", color: A,
        }}>{exporting === "html" ? "..." : "HTML"}</button>
      </div>

      {claimsLoading && (
        <div style={{ fontSize: 11, color: AL, padding: "4px 0 8px" }}>Loading claims data for {st}...</div>
      )}

      {/* Compliance status banner */}
      <Card accent={statusColor}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{ width: 10, height: 10, borderRadius: "50%", background: statusColor, flexShrink: 0 }} />
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 14, fontWeight: 700, color: statusColor }}>{statusLabel}</div>
            <div style={{ fontSize: 12, color: AL, marginTop: 2 }}>
              {STATE_NAMES[st]} -- {stats.totalCodes} codes analyzed across {CATEGORY_IDS.length} service categories
              {(cpraEm[st]?.length || 0) > 0 && <span> (locality-adjusted Medicare rates)</span>}
            </div>
            {pipeSummary && (
              <div style={{ fontSize: 11, color: AL, marginTop: 6, display: "flex", gap: 16, flexWrap: "wrap" }}>
                <span>Pipeline: median <strong style={{ color: pipeSummary.median_pct < 80 ? NEG : POS }}>{pipeSummary.median_pct.toFixed(1)}%</strong> MCR</span>
                <span>{pipeSummary.below_80_count.toLocaleString()} codes below 80%</span>
                {pipeSummary.worst_category && <span>Worst: {CATEGORY_META[pipeSummary.worst_category]?.label || pipeSummary.worst_category} ({pipeSummary.worst_category_pct?.toFixed(1)}%)</span>}
                {national && <span style={{ color: AL }}>National median: {national.median_pct.toFixed(1)}% across {national.state_count} states</span>}
              </div>
            )}
          </div>
        </div>
      </Card>

      {/* Category tabs */}
      <div style={{ display: "flex", gap: 4, marginBottom: 16, flexWrap: "wrap" }}>
        <Pill label="All" active={tab === "all"} onClick={() => setTab("all")} />
        {CATEGORY_IDS.map(catId => (
          <Pill key={catId} label={CATEGORY_META[catId].label} active={tab === catId} onClick={() => setTab(catId)} />
        ))}
      </div>

      {/* Summary KPIs */}
      <Card>
        <CH title="Rate Parity Summary" sub={`Weighted by CY2023 FFS claim volume${tab !== "all" ? ` -- ${CATEGORY_META[tab]?.label || ""}` : ""}`} />
        <div style={{ display: "flex", gap: 16, flexWrap: "wrap", justifyContent: "space-around", padding: "8px 0" }}>
          <Met label="Codes" value={String(filtered.length)} />
          <Met label="Wtd Avg % MCR" value={`${stats.weightedAvg.toFixed(1)}%`} color={stats.weightedAvg < 80 ? NEG : POS} />
          <Met label="Below 80%" value={String(stats.below80)} color={stats.below80 > 0 ? WARN : POS} />
          <Met label="Below 50%" value={String(stats.below50)} color={stats.below50 > 0 ? NEG : POS} />
          <Met label="Claims" value={fN(stats.totalClaims)} sub="CY2023 FFS" />
          <Met label="Beneficiaries" value={fN(stats.totalBene)} sub="patient-service events" />
        </div>
      </Card>

      {/* Threshold bar visualization */}
      {barData.length > 0 && (
        <Card>
          <CH title="Rate Distribution" sub="All codes with Medicare comparison, sorted by % of Medicare" />
          <div style={{ position: "relative", height: 80, background: SF, borderRadius: 6, overflow: "hidden" }}>
            <div style={{ position: "absolute", left: "25%", top: 0, bottom: 0, width: 1, background: `${NEG}44`, zIndex: 1 }} />
            <div style={{ position: "absolute", left: "40%", top: 0, bottom: 0, width: 1, background: `${POS}44`, zIndex: 1 }} />
            <div style={{ position: "absolute", left: "50%", top: 0, bottom: 0, width: 1, background: `${AL}22`, zIndex: 1 }} />
            <div style={{ position: "absolute", left: "25%", top: 2, fontSize: 9, color: NEG, fontFamily: FM, transform: "translateX(-100%)", paddingRight: 3, zIndex: 2 }}>50%</div>
            <div style={{ position: "absolute", left: "40%", top: 2, fontSize: 9, color: POS, fontFamily: FM, transform: "translateX(-100%)", paddingRight: 3, zIndex: 2 }}>80%</div>
            <div style={{ position: "absolute", left: "50%", top: 2, fontSize: 9, color: AL, fontFamily: FM, paddingRight: 3, zIndex: 2 }}>100%</div>
            <div style={{ display: "flex", alignItems: "flex-end", height: "100%", padding: "0 2px", gap: 1 }}>
              {barData.map((d, i) => {
                const cappedPct = Math.min(d.pct, 200);
                const h = Math.max(4, (cappedPct / 200) * 70);
                const barColor = d.flag === "critical" ? NEG : d.flag === "warn" ? WARN : POS;
                return (
                  <div key={i} title={`${d.code}: ${d.pct.toFixed(1)}% of Medicare`} style={{
                    flex: 1, minWidth: 2, maxWidth: 8, height: h,
                    background: barColor, borderRadius: "2px 2px 0 0", opacity: 0.8,
                  }} />
                );
              })}
            </div>
          </div>
        </Card>
      )}

      {/* Rate comparison table */}
      <Card>
        <CH title="Code-Level Rate Comparison" sub={`${filtered.length} codes | Click column headers to sort`} />
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
            <thead>
              <tr style={{ borderBottom: `2px solid ${BD}` }}>
                {([
                  { key: "hcpcs" as SortKey, label: "HCPCS", align: "left" },
                  { key: "desc" as SortKey, label: "Description", align: "left" },
                  { key: "category" as SortKey, label: "Category", align: "left" },
                  { key: "medicaidRate" as SortKey, label: "Medicaid", align: "right" },
                  { key: "medicareRate" as SortKey, label: "Medicare", align: "right" },
                  { key: "pctMedicare" as SortKey, label: "% MCR", align: "right" },
                  { key: "claims" as SortKey, label: "Claims", align: "right" },
                  { key: "bene" as SortKey, label: "Bene", align: "right" },
                ] as { key: SortKey; label: string; align: string }[]).map((col, ci) => (
                  <th key={ci} onClick={() => handleSort(col.key)} style={{
                    padding: "8px 6px", textAlign: col.align as "left" | "right" | "center",
                    fontSize: 10, fontWeight: 600, color: AL, fontFamily: FM,
                    cursor: "pointer", whiteSpace: "nowrap", userSelect: "none", letterSpacing: 0.3,
                  }}>
                    {col.label}
                    {sortKey === col.key && <span style={{ marginLeft: 3 }}>{sortAsc ? "↑" : "↓"}</span>}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {filtered.map((r, ri) => {
                const rowBg = r.flag === "critical" ? "#FDF2F208" : r.flag === "warn" ? "#FFFBEB08" : "transparent";
                return (
                  <tr key={r.hcpcs} style={{ background: ri % 2 === 0 ? rowBg : `${SF}60`, borderBottom: `1px solid ${BD}40` }}>
                    <td style={{ padding: "6px", fontFamily: FM, fontSize: 11, color: A, whiteSpace: "nowrap" }}>{r.hcpcs}</td>
                    <td style={{ padding: "6px", color: A, maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.desc}</td>
                    <td style={{ padding: "6px" }}>
                      <Badge text={r.categoryLabel} color={r.category === "primary_care" ? cB : r.category === "obgyn" ? "#7B3FA0" : "#2563EB"} />
                    </td>
                    <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11 }}>${r.medicaidRate.toFixed(2)}</td>
                    <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11, color: r.medicareRate !== null ? A : AL }}>
                      {r.medicareRate !== null ? `$${r.medicareRate.toFixed(2)}` : "N/A"}
                    </td>
                    <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11, fontWeight: 600,
                      color: r.flag === "critical" ? NEG : r.flag === "warn" ? WARN : r.flag === "pass" ? POS : AL }}>
                      {r.pctMedicare !== null ? `${r.pctMedicare.toFixed(1)}%` : "N/A"}
                    </td>
                    <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11, color: AL }}>
                      {r.claims > 0 ? fN(r.claims) : "-"}
                    </td>
                    <td style={{ padding: "6px", textAlign: "right", fontFamily: FM, fontSize: 11, color: AL }}>
                      {r.bene > 0 ? fN(r.bene) : "-"}
                    </td>
                  </tr>
                );
              })}
              {filtered.length === 0 && (
                <tr><td colSpan={8} style={{ padding: 24, textAlign: "center", color: AL, fontSize: 12 }}>
                  No CPRA codes found for {STATE_NAMES[st]} in this category. This may indicate limited FFS coverage or missing fee schedule data.
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Data quality panel */}
      <Card>
        <CH title="Data Quality and Limitations" sub="Transparency disclosures per 42 CFR 447.203" />
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, fontSize: 12, color: AL, lineHeight: 1.7 }}>
          <div>
            <div style={{ fontWeight: 600, color: A, marginBottom: 4, fontSize: 11 }}>Coverage</div>
            <div>E/M code scope: <strong>{allCodes.size}</strong> codes (CMS CY2025 list)</div>
            <div>Codes with rates in {st}: <strong>{rows.length}</strong></div>
            <div>Service year: <strong>2023</strong> T-MSIS (FFS claims only)</div>
            <div>MCO penetration: {ffsShare !== null && ffsShare !== undefined && !isNaN(Number(ffsShare))
              ? <span>{(Number(ffsShare) * 100).toFixed(0)}% FFS / {(100 - Number(ffsShare) * 100).toFixed(0)}% managed care</span>
              : <span style={{ color: WARN }}>FFS share unavailable</span>
            }</div>
          </div>
          <div>
            <div style={{ fontWeight: 600, color: A, marginBottom: 4, fontSize: 11 }}>Rate Sources</div>
            <div>Pre-computed (locality-adjusted): <strong>{stats.fsCodes}</strong> codes</div>
            <div>Fallback (client-side): <strong>{stats.precomputedCodes}</strong> codes</div>
            <div>Medicare benchmark: <strong>CY2026 PFS</strong> (CF=$33.4009)</div>
            <div>Medicaid vs Medicare from <strong>cpra_engine.py</strong> pipeline</div>
          </div>
          <div>
            <div style={{ fontWeight: 600, color: A, marginBottom: 4, fontSize: 11 }}>Data Quality Flags</div>
            {stateDqFlags.length > 0 ? (
              <>
                {dqSummary.BELOW_50PCT && <div style={{ color: NEG }}>{dqSummary.BELOW_50PCT} codes below 50% of Medicare</div>}
                {dqSummary.STALE_RATE && <div style={{ color: WARN }}>{dqSummary.STALE_RATE} codes with stale rates</div>}
                {dqSummary.MISSING_MEDICARE && <div>{dqSummary.MISSING_MEDICARE} codes missing Medicare rate</div>}
                {dqSummary.METHODOLOGY_RISK && <div style={{ color: WARN }}>Methodology risk: pct-of-charge pricing</div>}
                {dqSummary.LOW_COVERAGE && <div style={{ color: WARN }}>Low coverage: fewer than 1,000 matched codes</div>}
                {dqSummary.ABOVE_MEDICARE && <div style={{ color: POS }}>{dqSummary.ABOVE_MEDICARE} codes above Medicare</div>}
              </>
            ) : (
              <div>No quality flags for this state</div>
            )}
          </div>
          <div>
            <div style={{ fontWeight: 600, color: A, marginBottom: 4, fontSize: 11 }}>Known Limitations</div>
            {dqStateNotes[st]?.notes?.length > 0 ? (
              dqStateNotes[st].notes.map((note, i) => (
                <div key={i} style={{ color: WARN, marginBottom: 2 }}>{note}</div>
              ))
            ) : null}
            <div>Beneficiary counts = patient-service events (not unique headcount)</div>
            <div>FFS-only: missing managed care, inpatient, pharmacy, LTSS</div>
            <div>T-MSIS effective rates may differ from published fee schedules</div>
          </div>
        </div>
      </Card>

      {/* Methodology reference */}
      {stateConv && (
        <Card>
          <CH title="State Methodology" sub={`${STATE_NAMES[st]} rate-setting approach`} />
          <div style={{ fontSize: 12, color: AL, lineHeight: 1.7 }}>
            <div><strong>Methodology:</strong> {stateConv.methodology}</div>
            {stateConv.methodology_detail && <div style={{ marginTop: 4 }}>{stateConv.methodology_detail}</div>}
            {stateConv.update_frequency && <div style={{ marginTop: 4 }}><strong>Update frequency:</strong> {stateConv.update_frequency}</div>}
            {stateConv.gpci_approach && <div><strong>GPCI approach:</strong> {stateConv.gpci_approach}</div>}
            {stateConv.fee_schedule_type && <div><strong>Fee schedule type:</strong> {stateConv.fee_schedule_type}</div>}
          </div>
          <div style={{ marginTop: 16, padding: "12px 16px", background: SF, borderRadius: 6, fontSize: 11, color: AL, lineHeight: 1.6 }}>
            <strong style={{ color: A }}>Regulatory Citation</strong><br />
            42 CFR 447.203 -- Ensuring Access to Medicaid Services Final Rule<br />
            Requires states to publish comparative payment rate analyses and demonstrate payment sufficiency for primary care, OB/GYN, and outpatient mental health/SUD services.<br />
            Compliance deadline: July 1, 2026
          </div>
        </Card>
      )}
      </>
      )}
    </div>
  );
}
