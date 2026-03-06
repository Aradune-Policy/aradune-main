// /api/chat.js — Vercel Serverless Function
// Proxies requests to Anthropic API with tool use for Aradune data access
// Rate-limited, token-gated, with smart context management

import Anthropic from "@anthropic-ai/sdk";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ── Data Access Layer ─────────────────────────────────────────────────
// Reads from Vercel's bundled static files (public/data/)
// These are deployed alongside the function — no external DB needed.

const DATA_DIR = join(process.cwd(), "public", "data");

function loadJSON(filename) {
  const path = join(DATA_DIR, filename);
  if (!existsSync(path)) return null;
  try { return JSON.parse(readFileSync(path, "utf-8")); } catch { return null; }
}

// Lazy-loaded data cache (persists across warm invocations)
let _hcpcs = null;
let _medicare = null;
let _bls = null;
let _quality = null;
let _crosswalk = null;

function getHCPCS() { if (!_hcpcs) _hcpcs = loadJSON("hcpcs.json"); return _hcpcs; }
function getMedicare() { if (!_medicare) _medicare = loadJSON("medicare_rates.json"); return _medicare; }
function getBLS() { if (!_bls) _bls = loadJSON("bls_wages.json"); return _bls; }
function getQuality() { if (!_quality) _quality = loadJSON("quality_measures.json"); return _quality; }
function getCrosswalk() { if (!_crosswalk) _crosswalk = loadJSON("soc_hcpcs_crosswalk.json"); return _crosswalk; }

// ── Tool Implementations ──────────────────────────────────────────────

function lookupRate(state, code) {
  const hcpcs = getHCPCS();
  if (!hcpcs || !Array.isArray(hcpcs)) return { error: "HCPCS data not loaded" };
  const record = hcpcs.find(r => (r.code || r.c) === code);
  if (!record) return { error: `Code ${code} not found in T-MSIS data` };

  // Handle different data formats
  let stateRate = null;
  let allRates = {};

  const ratesObj = record.rates || record.r;
  if (ratesObj && typeof ratesObj === "object") {
    stateRate = ratesObj[state] || null;
    allRates = ratesObj;
  }

  const rates = Object.values(allRates).filter(r => r > 0).sort((a, b) => a - b);
  const median = rates.length > 0 ? rates[Math.floor(rates.length / 2)] : null;
  const n = rates.length;

  return {
    code,
    state,
    rate: stateRate,
    source: "T-MSIS actual-paid (avg per claim)",
    description: record.d || record.desc || record.description || null,
    national_median: median,
    national_min: rates[0] || null,
    national_max: rates[rates.length - 1] || null,
    states_with_data: n,
    percentile: stateRate && n > 0
      ? Math.round((rates.filter(r => r <= stateRate).length / n) * 100)
      : null,
  };
}

function lookupMedicare(code) {
  const mcr = getMedicare();
  if (!mcr) return { error: "Medicare PFS data not loaded" };

  let record = null;
  if (Array.isArray(mcr)) {
    record = mcr.find(r => (r.code || r.c || r.hcpcs) === code);
  } else if (mcr.rates && mcr.rates[code]) {
    record = mcr.rates[code];
  } else if (mcr[code]) {
    record = mcr[code];
  }
  if (!record) return { error: `Code ${code} not found in Medicare PFS` };

  return {
    code,
    source: "CY2025 Medicare Physician Fee Schedule",
    nf_rate: record.r || record.nf_rate || record.rate || null,
    f_rate: record.fr || record.f_rate || record.facility || null,
    total_rvu: record.rvu || record.nf_rvu || record.total_rvu || null,
    work_rvu: record.w || record.work_rvu || record.work || null,
    pe_rvu: record.pe_rvu || record.pe || null,
    mp_rvu: record.mp_rvu || record.mp || null,
    description: record.d || record.desc || record.description || null,
    global_days: record.global_days || record.global || null,
    pc_tc: record.pc_tc || null,
    conversion_factor: 32.7442, // CY2025 CF
  };
}

function compareStates(code, states) {
  const hcpcs = getHCPCS();
  if (!hcpcs || !Array.isArray(hcpcs)) return { error: "HCPCS data not loaded" };
  const record = hcpcs.find(r => (r.code || r.c) === code);
  if (!record) return { error: `Code ${code} not found` };

  const mcr = lookupMedicare(code);
  const medicareRate = mcr.nf_rate || null;

  let allRates = {};
  const rObj = record.rates || record.r;
  if (rObj && typeof rObj === "object") allRates = rObj;

  // If no specific states requested, return all
  const targetStates = states && states.length > 0
    ? states
    : Object.keys(allRates).sort();

  const results = targetStates.map(st => ({
    state: st,
    rate: allRates[st] || null,
    pct_medicare: allRates[st] && medicareRate ? Math.round((allRates[st] / medicareRate) * 100) : null,
  })).filter(r => r.rate != null);

  const rates = results.map(r => r.rate).sort((a, b) => a - b);

  return {
    code,
    description: record.d || record.desc || null,
    medicare_rate: medicareRate,
    states: results.sort((a, b) => a.rate - b.rate),
    summary: {
      n: results.length,
      min: rates[0],
      p25: rates[Math.floor(rates.length * 0.25)],
      median: rates[Math.floor(rates.length * 0.5)],
      p75: rates[Math.floor(rates.length * 0.75)],
      max: rates[rates.length - 1],
      mean: rates.length > 0 ? Math.round(rates.reduce((a, b) => a + b, 0) / rates.length * 100) / 100 : null,
    },
  };
}

function getQualityMeasure(measureId, state) {
  const qual = getQuality();
  if (!qual) return { error: "Quality data not loaded" };

  if (measureId) {
    const meta = qual.measures?.[measureId];
    const rates = qual.rates?.[measureId];
    const hcpcsLink = qual.measure_hcpcs?.[measureId];
    if (!meta) return { error: `Measure ${measureId} not found` };

    return {
      measure: measureId,
      name: meta.name,
      domain: meta.domain,
      rate_definition: meta.rate_def,
      national_median: meta.median,
      states_reporting: meta.n_states,
      state_rate: state && rates ? rates[state] || null : null,
      linked_hcpcs: hcpcsLink?.codes || null,
      hcpcs_description: hcpcsLink?.desc || null,
      all_state_rates: rates || {},
    };
  }

  // Return all measures for a state
  if (state) {
    const results = Object.entries(qual.measures).map(([id, meta]) => ({
      measure: id,
      name: meta.name,
      domain: meta.domain,
      rate: qual.rates?.[id]?.[state] || null,
      median: meta.median,
      above_median: qual.rates?.[id]?.[state] != null && meta.median != null
        ? qual.rates[id][state] >= meta.median : null,
    })).filter(m => m.rate != null);
    return { state, measures: results, n: results.length };
  }

  return { error: "Provide measureId or state" };
}

function getWageData(state, socCode) {
  const bls = getBLS();
  if (!bls) return { error: "BLS wage data not loaded" };

  if (state && socCode) {
    const wage = bls.states?.[state]?.[socCode];
    const natl = bls.national?.[socCode];
    if (!wage) return { error: `No wage data for SOC ${socCode} in ${state}` };
    return {
      state, soc_code: socCode, title: wage.title,
      source: "BLS OEWS May 2024",
      hourly: { median: wage.h_median, mean: wage.h_mean, p10: wage.h_p10, p25: wage.h_p25, p75: wage.h_p75, p90: wage.h_p90 },
      annual_median: wage.a_median,
      employment: wage.emp,
      national: natl ? { median: natl.h_median, mean: natl.h_mean, employment: natl.emp } : null,
    };
  }

  // All occupations for a state
  if (state) {
    const stateData = bls.states?.[state];
    if (!stateData) return { error: `No BLS data for ${state}` };
    return {
      state,
      occupations: Object.entries(stateData).map(([soc, w]) => ({
        soc, title: w.title, median: w.h_median, employment: w.emp,
      })),
    };
  }

  // All states for an occupation
  if (socCode) {
    const results = Object.entries(bls.states || {}).map(([st, occs]) => {
      const w = occs[socCode];
      return w ? { state: st, median: w.h_median, mean: w.h_mean, employment: w.emp } : null;
    }).filter(Boolean).sort((a, b) => a.median - b.median);
    return { soc_code: socCode, title: bls.occupations?.[socCode], states: results };
  }

  return { error: "Provide state and/or socCode" };
}

function searchCodes(query) {
  const hcpcs = getHCPCS();
  const mcr = getMedicare();
  if (!hcpcs && !mcr) return { error: "No code data loaded" };

  const q = query.toLowerCase();
  const results = [];
  const seen = new Set();

  // Search Medicare data first (has descriptions)
  if (mcr) {
    const ratesMap = mcr.rates || mcr;
    const entries = Array.isArray(ratesMap)
      ? ratesMap.map(r => [r.code || r.c || r.hcpcs || "", r])
      : Object.entries(ratesMap);
    entries.forEach(([key, r]) => {
      const code = r.code || r.c || r.hcpcs || key || "";
      const desc = (r.d || r.desc || r.description || "").toLowerCase();
      if ((code.toLowerCase().includes(q) || desc.includes(q)) && !seen.has(code)) {
        seen.add(code);
        results.push({ code, description: r.d || r.desc || r.description, source: "Medicare PFS" });
      }
    });
  }

  // Also search T-MSIS
  if (hcpcs && Array.isArray(hcpcs)) {
    hcpcs.forEach(r => {
      const code = r.code || r.c || "";
      const desc = (r.desc || r.d || "").toLowerCase();
      if ((code.toLowerCase().includes(q) || desc.includes(q)) && !seen.has(code)) {
        seen.add(code);
        results.push({ code, description: r.desc || r.d, source: "T-MSIS" });
      }
    });
  }

  return { query, results: results.slice(0, 25), total: results.length };
}

// ── Tool Definitions (for Anthropic API) ──────────────────────────────

const TOOLS = [
  {
    name: "lookup_rate",
    description: "Get the Medicaid reimbursement rate for a specific HCPCS code in a specific state. Returns T-MSIS actual-paid rate (average per claim), national percentile, and distribution stats.",
    input_schema: {
      type: "object",
      properties: {
        state: { type: "string", description: "Two-letter state code (e.g., FL, NY, CA)" },
        code: { type: "string", description: "HCPCS/CPT code (e.g., 99213, 90834, D1351)" },
      },
      required: ["state", "code"],
    },
  },
  {
    name: "lookup_medicare",
    description: "Get the Medicare Physician Fee Schedule rate and RVU breakdown for a HCPCS code. Returns CY2025 non-facility and facility rates, work/PE/MP RVUs, and conversion factor.",
    input_schema: {
      type: "object",
      properties: {
        code: { type: "string", description: "HCPCS/CPT code" },
      },
      required: ["code"],
    },
  },
  {
    name: "compare_states",
    description: "Compare Medicaid rates for a specific code across multiple states. Returns rates sorted low to high with percentile stats and % of Medicare. If states array is empty, returns all available states.",
    input_schema: {
      type: "object",
      properties: {
        code: { type: "string", description: "HCPCS/CPT code" },
        states: { type: "array", items: { type: "string" }, description: "Array of state codes to compare. Empty array = all states." },
      },
      required: ["code"],
    },
  },
  {
    name: "get_quality",
    description: "Get CMS Medicaid Core Set quality measure data. Can look up a specific measure (with optional state), or get all measures for a state. Measures are linked to HCPCS codes.",
    input_schema: {
      type: "object",
      properties: {
        measure_id: { type: "string", description: "Core Set measure abbreviation (e.g., WCV-CH, FUH-AD, SFM-CH)" },
        state: { type: "string", description: "Two-letter state code" },
      },
    },
  },
  {
    name: "get_wages",
    description: "Get BLS occupational wage data (May 2024). Can look up a specific occupation in a state, all occupations in a state, or one occupation across all states. Key SOCs: 31-1120 (Home Health Aides), 29-2061 (LPNs), 29-1141 (RNs), 21-1014 (MH Counselors), 31-9091 (Dental Assistants).",
    input_schema: {
      type: "object",
      properties: {
        state: { type: "string", description: "Two-letter state code" },
        soc_code: { type: "string", description: "SOC occupation code (e.g., 31-1120)" },
      },
    },
  },
  {
    name: "search_codes",
    description: "Search for HCPCS codes by keyword or partial code. Returns matching codes with descriptions. Use this when the user mentions a service name rather than a specific code.",
    input_schema: {
      type: "object",
      properties: {
        query: { type: "string", description: "Search term (code fragment or service description keyword)" },
      },
      required: ["query"],
    },
  },
];

// ── Tool Executor ─────────────────────────────────────────────────────

function executeTool(name, input) {
  switch (name) {
    case "lookup_rate": return lookupRate(input.state, input.code);
    case "lookup_medicare": return lookupMedicare(input.code);
    case "compare_states": return compareStates(input.code, input.states || []);
    case "get_quality": return getQualityMeasure(input.measure_id, input.state);
    case "get_wages": return getWageData(input.state, input.soc_code);
    case "search_codes": return searchCodes(input.query);
    default: return { error: `Unknown tool: ${name}` };
  }
}

// ── Auth & Rate Limiting ──────────────────────────────────────────────
// Three auth paths:
// 1. ADMIN_KEY — master key for admin
// 2. ANALYST_TOKENS — comma-separated static tokens (backwards compat)
// 3. Signed tokens — base64url(payload).hmac, verified via JWT_SECRET,
//    then Stripe subscription check on extracted customer ID

import { createHmac } from "crypto";
import Stripe from "stripe";

function base64url(buf) {
  return buf.toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64urlDecode(str) {
  const padded = str.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - str.length % 4) % 4);
  return Buffer.from(padded, "base64");
}

// Cache Stripe subscription status per customer per warm invocation
const subscriptionCache = {};

async function checkStripeSubscription(customerId) {
  const now = Date.now();
  const cached = subscriptionCache[customerId];
  if (cached && now - cached.ts < 300000) return cached.active; // 5-min cache

  try {
    const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
    const subs = await stripe.subscriptions.list({
      customer: customerId,
      status: "active",
      limit: 1,
    });
    const active = subs.data.length > 0;
    subscriptionCache[customerId] = { active, ts: now };
    return active;
  } catch {
    return false;
  }
}

async function validateToken(token) {
  if (!token) return false;

  // Admin key
  if (token === process.env.ADMIN_KEY) return true;

  // Preview gate password (allows anyone who passed the site password gate)
  const previewToken = process.env.PREVIEW_TOKEN || "mediquiad";
  if (token === previewToken) return true;

  // Static tokens (backwards compat)
  const validTokens = (process.env.ANALYST_TOKENS || "").split(",").map(t => t.trim()).filter(Boolean);
  if (validTokens.includes(token)) return true;

  // Signed token: payload.signature
  if (token.includes(".") && process.env.JWT_SECRET) {
    const [payloadB64, sigB64] = token.split(".");
    if (!payloadB64 || !sigB64) return false;

    const expectedSig = base64url(
      createHmac("sha256", process.env.JWT_SECRET).update(payloadB64).digest()
    );
    if (sigB64 !== expectedSig) return false;

    try {
      const payload = JSON.parse(base64urlDecode(payloadB64).toString("utf-8"));
      if (!payload.cid) return false;
      return await checkStripeSubscription(payload.cid);
    } catch {
      return false;
    }
  }

  return false;
}

// Simple in-memory rate limit (resets when function cold-starts, ~5-15 min)
// For production: use Vercel KV
const rateLimits = {};
const MAX_QUERIES_PER_HOUR = 30;

function checkRateLimit(token) {
  const now = Date.now();
  const hourAgo = now - 3600000;

  if (!rateLimits[token]) rateLimits[token] = [];
  rateLimits[token] = rateLimits[token].filter(t => t > hourAgo);

  if (rateLimits[token].length >= MAX_QUERIES_PER_HOUR) {
    return { allowed: false, remaining: 0, reset: Math.ceil((rateLimits[token][0] + 3600000 - now) / 60000) };
  }

  rateLimits[token].push(now);
  return { allowed: true, remaining: MAX_QUERIES_PER_HOUR - rateLimits[token].length };
}

// ── Load System Prompt ────────────────────────────────────────────────

function getSystemPrompt() {
  // Try loading from file, fall back to embedded
  const promptPath = join(DATA_DIR, "system_prompt.md");
  let prompt;
  if (existsSync(promptPath)) {
    prompt = readFileSync(promptPath, "utf-8");
  } else {
    prompt = "You are the Aradune Policy Analyst, an AI specialized in Medicaid rate-setting and fee schedule analysis. Use the available tools to ground your answers in real data from Aradune's dataset. Always cite data sources and vintages.";
  }
  // Append FL methodology addendum if available
  const addendumPath = join(DATA_DIR, "fl_methodology_addendum.md");
  if (existsSync(addendumPath)) {
    prompt += "\n\n" + readFileSync(addendumPath, "utf-8");
  }
  return prompt;
}

// ── API Handler ───────────────────────────────────────────────────────

export default async function handler(req, res) {
  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  // Auth
  const authHeader = req.headers.authorization || "";
  const token = authHeader.replace("Bearer ", "").trim();

  if (!(await validateToken(token))) {
    return res.status(401).json({
      error: "Invalid or missing access token",
      message: "The Policy Analyst requires a valid access token. Visit aradune.co/subscribe to get access.",
    });
  }

  // Rate limit
  const rl = checkRateLimit(token);
  if (!rl.allowed) {
    return res.status(429).json({
      error: "Rate limit exceeded",
      message: `Maximum ${MAX_QUERIES_PER_HOUR} queries per hour. Try again in ${rl.reset} minutes.`,
      remaining: 0,
    });
  }

  const { messages } = req.body;
  if (!messages || !Array.isArray(messages)) {
    return res.status(400).json({ error: "messages array required" });
  }

  // Limit conversation history to last 20 messages to control context size
  const trimmedMessages = messages.slice(-20);

  try {
    // Initial API call with tools
    let response = await client.messages.create({
      model: "claude-sonnet-4-5-20250514",
      max_tokens: 4096,
      system: getSystemPrompt(),
      tools: TOOLS,
      messages: trimmedMessages,
    });

    // Agentic tool-use loop: keep going until Claude stops calling tools
    const MAX_TOOL_ROUNDS = 8;
    let rounds = 0;
    let allMessages = [...trimmedMessages];

    while (response.stop_reason === "tool_use" && rounds < MAX_TOOL_ROUNDS) {
      rounds++;

      // Extract tool use blocks
      const toolUses = response.content.filter(b => b.type === "tool_use");
      const toolResults = toolUses.map(tu => ({
        type: "tool_result",
        tool_use_id: tu.id,
        content: JSON.stringify(executeTool(tu.name, tu.input)),
      }));

      // Add assistant response + tool results to conversation
      allMessages.push({ role: "assistant", content: response.content });
      allMessages.push({ role: "user", content: toolResults });

      // Continue the conversation
      response = await client.messages.create({
        model: "claude-sonnet-4-5-20250514",
        max_tokens: 4096,
        system: getSystemPrompt(),
        tools: TOOLS,
        messages: allMessages,
      });
    }

    // Extract final text response
    const textBlocks = response.content.filter(b => b.type === "text");
    const text = textBlocks.map(b => b.text).join("\n");

    // Track token usage for cost monitoring
    const usage = response.usage || {};

    return res.status(200).json({
      response: text,
      usage: {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        tool_rounds: rounds,
      },
      rate_limit: { remaining: rl.remaining },
    });

  } catch (error) {
    console.error("Anthropic API error:", error);

    if (error.status === 429) {
      return res.status(429).json({ error: "API rate limit. Try again in a moment." });
    }

    return res.status(500).json({
      error: "Policy Analyst encountered an error",
      message: error.message || "Unknown error",
    });
  }
}

export const config = {
  maxDuration: 60, // Allow up to 60s for complex multi-tool queries
};
