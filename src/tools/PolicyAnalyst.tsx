import React, { useState, useRef, useEffect, useCallback } from "react";
import type { ChatMessage, ChatUsage } from "../types";

// ── Design System ───────────────────────────────────────────────────────
const A = "#0A2540";
const AL = "#425A70";
const POS = "#2E6B4A";
const NEG = "#A4262C";
const SF = "#F5F7F5";
const BD = "#E4EAE4";
const WH = "#fff";
const cB = "#2E6B4A";
const cO = "#C4590A";
const FM = "'SF Mono',Menlo,monospace";
const SH = "0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.03)";

// ── Token Management ────────────────────────────────────────────────────
// Store in localStorage. In production, replace with proper session management.
function getToken(): string { try { return localStorage.getItem("aradune_token") || ""; } catch (_e) { return ""; } }
function setToken(t: string): void { try { localStorage.setItem("aradune_token", t); } catch (_e) { /* noop */ } }

// ── Markdown-lite renderer ──────────────────────────────────────────────
function renderMarkdown(text: string): React.ReactNode[] | null {
  if (!text) return null;
  const lines: string[] = text.split("\n");
  const elements: React.ReactNode[] = [];
  let inCode = false;
  let codeBlock: string[] = [];
  let codeLang = "";

  lines.forEach((line: string, i: number) => {
    if (line.startsWith("```")) {
      if (inCode) {
        elements.push(
          <pre key={`code-${i}`} style={{ background:"rgba(10,37,64,0.06)",borderRadius:6,padding:"10px 12px",overflow:"auto",fontSize:11,fontFamily:FM,lineHeight:1.5,margin:"6px 0",border:`1px solid ${BD}` }}>
            {codeLang && <div style={{ fontSize:8,color:AL,marginBottom:4,textTransform:"uppercase" }}>{codeLang}</div>}
            <code>{codeBlock.join("\n")}</code>
          </pre>
        );
        codeBlock = [];
        codeLang = "";
        inCode = false;
      } else {
        inCode = true;
        codeLang = line.slice(3).trim();
      }
      return;
    }
    if (inCode) { codeBlock.push(line); return; }
    if (line.startsWith("### ")) {
      elements.push(<div key={i} style={{ fontSize:12,fontWeight:700,color:A,marginTop:10,marginBottom:2 }}>{line.slice(4)}</div>);
    } else if (line.startsWith("## ")) {
      elements.push(<div key={i} style={{ fontSize:13,fontWeight:700,color:A,marginTop:12,marginBottom:4 }}>{line.slice(3)}</div>);
    } else if (line.startsWith("# ")) {
      elements.push(<div key={i} style={{ fontSize:15,fontWeight:700,color:A,marginTop:12,marginBottom:4 }}>{line.slice(2)}</div>);
    } else if (line.startsWith("- ") || line.startsWith("* ")) {
      elements.push(<div key={i} style={{ paddingLeft:12,position:"relative",marginBottom:2 }}><span style={{ position:"absolute",left:0 }}>•</span>{renderInline(line.slice(2))}</div>);
    } else if (/^\d+\.\s/.test(line)) {
      const num = line.match(/^(\d+)\.\s/)![1];
      elements.push(<div key={i} style={{ paddingLeft:16,position:"relative",marginBottom:2 }}><span style={{ position:"absolute",left:0,fontFamily:FM,fontSize:10,color:AL }}>{num}.</span>{renderInline(line.replace(/^\d+\.\s/,""))}</div>);
    } else if (line.startsWith("|") && line.endsWith("|")) {
      // Simple table row
      const cells = line.split("|").filter((c: string) => c.trim()).map((c: string) => c.trim());
      if (cells.some((c: string) => /^[-:]+$/.test(c))) return; // separator row
      elements.push(
        <div key={i} style={{ display:"flex",gap:1,fontSize:10 }}>
          {cells.map((c: string, j: number) => <div key={j} style={{ flex:1,padding:"3px 6px",background:i===0||elements.length<3?SF:"transparent",fontWeight:i===0?"600":"400",fontFamily:FM,borderBottom:`1px solid ${BD}` }}>{c}</div>)}
        </div>
      );
    } else if (line.startsWith("> ")) {
      elements.push(<div key={i} style={{ borderLeft:`3px solid ${cB}`,paddingLeft:10,color:AL,fontStyle:"italic",margin:"4px 0" }}>{renderInline(line.slice(2))}</div>);
    } else if (line.trim() === "") {
      elements.push(<div key={i} style={{ height:6 }}/>);
    } else {
      elements.push(<div key={i} style={{ marginBottom:2 }}>{renderInline(line)}</div>);
    }
  });

  return elements;
}

function renderInline(text: string): React.ReactNode[] {
  // Handle **bold**, `code`, and *italic*
  const parts: React.ReactNode[] = [];
  let remaining = text;
  let key = 0;

  while (remaining.length > 0) {
    // Bold
    const boldMatch = remaining.match(/\*\*(.+?)\*\*/);
    // Code
    const codeMatch = remaining.match(/`(.+?)`/);
    // Pick earliest match
    let earliest: RegExpMatchArray | null = null;
    let type: string | null = null;
    let earliestIdx = Infinity;

    if (boldMatch) { const idx = boldMatch.index ?? 0; if (idx < earliestIdx) { earliest = boldMatch; type = "bold"; earliestIdx = idx; } }
    if (codeMatch) { const idx = codeMatch.index ?? 0; if (idx < earliestIdx) { earliest = codeMatch; type = "code"; earliestIdx = idx; } }

    if (!earliest) {
      parts.push(<span key={key++}>{remaining}</span>);
      break;
    }

    if ((earliest.index ?? 0) > 0) {
      parts.push(<span key={key++}>{remaining.slice(0, earliest.index ?? 0)}</span>);
    }

    if (type === "bold") {
      parts.push(<b key={key++}>{earliest[1]}</b>);
    } else if (type === "code") {
      parts.push(<code key={key++} style={{ fontFamily:FM,background:SF,padding:"1px 4px",borderRadius:3,fontSize:"0.9em" }}>{earliest[1]}</code>);
    }

    remaining = remaining.slice((earliest.index ?? 0) + earliest[0].length);
  }

  return parts;
}

// ── Starter Prompts ─────────────────────────────────────────────────────
const STARTERS = [
  { q: "What does Florida pay for 99213 vs Medicare?", icon: "🔍" },
  { q: "Compare behavioral health rates across southeastern states", icon: "📊" },
  { q: "How should we price a new code that replaced D9248?", icon: "💡" },
  { q: "What's the implied wage for home health aides in Texas?", icon: "⚖" },
  { q: "Estimate fiscal impact of moving PT codes to 80% of Medicare in FL", icon: "📈" },
  { q: "Which states score worst on well-child visit quality measures?", icon: "🏥" },
];

// ── Main Component ──────────────────────────────────────────────────────
export default function PolicyAnalyst() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [token, setTokenState] = useState(getToken());
  const [showAuth, setShowAuth] = useState(!getToken());
  const [error, setError] = useState<string | null>(null);
  const [usage, setUsage] = useState<ChatUsage | null>(null);
  const [activating, setActivating] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Handle ?session= URL param from Stripe redirect
  useEffect(() => {
    const hash = window.location.hash;
    const match = hash.match(/[?&]session=(cs_[^&]+)/);
    if (!match) return;

    const sessionId = match[1];
    // Clean URL param
    window.location.hash = hash.replace(/[?&]session=cs_[^&]+/, "");

    setActivating(true);
    fetch(`/api/activate?session=${sessionId}`)
      .then(r => r.json())
      .then(data => {
        if (data.token) {
          setToken(data.token);
          setTokenState(data.token);
          setShowAuth(false);
          setError(null);
        } else {
          setError(data.error || "Activation failed");
        }
      })
      .catch(() => setError("Connection error during activation"))
      .finally(() => setActivating(false));
  }, []);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  const handleTokenSubmit = (t: string) => {
    setToken(t);
    setTokenState(t);
    setShowAuth(false);
    setError(null);
  };

  const sendMessage = useCallback(async (text: string) => {
    if (!text.trim() || loading) return;

    const userMsg: ChatMessage = { role: "user", content: text.trim() };
    const newMessages = [...messages, userMsg];
    setMessages(newMessages);
    setInput("");
    setLoading(true);
    setError(null);

    try {
      // Build API messages (just role + content pairs)
      const apiMessages = newMessages.map(m => ({
        role: m.role,
        content: m.content,
      }));

      const resp = await fetch("/api/chat", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`,
        },
        body: JSON.stringify({ messages: apiMessages }),
      });

      const data = await resp.json();

      if (!resp.ok) {
        if (resp.status === 401) {
          setShowAuth(true);
          setError(data.message || "Invalid token");
        } else if (resp.status === 429) {
          setError(data.message || "Rate limit exceeded");
        } else {
          setError(data.message || data.error || "Something went wrong");
        }
        setLoading(false);
        return;
      }

      setMessages([...newMessages, { role: "assistant", content: data.response } as ChatMessage]);
      setUsage(data.usage);
    } catch (e) {
      setError(`Connection error: ${e instanceof Error ? e.message : String(e)}`);
    }

    setLoading(false);
  }, [messages, loading, token]);

  // ── Auth Screen ───────────────────────────────────────────────────────
  if (showAuth) {
    return (
      <div style={{ maxWidth:480,margin:"60px auto",padding:"0 16px",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A }}>
        <div style={{ background:WH,borderRadius:12,boxShadow:SH,border:`1px solid ${BD}`,overflow:"hidden" }}>
          <div style={{ padding:"24px 24px 0",textAlign:"center" }}>
            <div style={{ fontSize:20,fontWeight:600,marginBottom:4 }}>AI Tier: Policy Analyst</div>
            <div style={{ fontSize:11,color:AL,lineHeight:1.6,maxWidth:340,margin:"0 auto" }}>
              This tool uses Claude AI with Aradune's full dataset. Access is
              currently limited while we finalize the subscription model.
            </div>
          </div>

          <div style={{ padding:"20px 24px" }}>
            {activating && <div style={{ padding:"8px 12px",background:"rgba(46,107,74,0.06)",border:`1px solid rgba(46,107,74,0.15)`,borderRadius:6,fontSize:11,color:POS,marginBottom:12 }}>Activating your subscription...</div>}
            {error && <div style={{ padding:"8px 12px",background:"rgba(164,38,44,0.06)",border:`1px solid rgba(164,38,44,0.15)`,borderRadius:6,fontSize:11,color:NEG,marginBottom:12 }}>{error}</div>}

            <div style={{ marginBottom:12 }}>
              <label style={{ fontSize:10,fontWeight:600,color:AL,display:"block",marginBottom:4 }}>Access Token</label>
              <input
                type="password"
                placeholder="Enter your access token"
                defaultValue={token}
                onKeyDown={e => e.key === "Enter" && handleTokenSubmit(e.currentTarget.value)}
                onChange={e => setTokenState(e.currentTarget.value)}
                style={{ width:"100%",padding:"10px 12px",border:`1px solid ${BD}`,borderRadius:6,fontSize:13,fontFamily:FM,boxSizing:"border-box" }}
              />
            </div>
            <button
              onClick={() => handleTokenSubmit(token)}
              style={{ width:"100%",padding:"10px",background:cB,color:WH,border:"none",borderRadius:6,fontSize:12,fontWeight:600,cursor:"pointer" }}>
              Connect
            </button>
          </div>

          <div style={{ padding:"16px 24px",background:SF,borderTop:`1px solid ${BD}` }}>
            <div style={{ fontSize:10,fontWeight:600,color:A,marginBottom:6 }}>What's included</div>
            <div style={{ fontSize:10,color:AL,lineHeight:1.7 }}>
              {["Rate lookup across all states + Medicare","Cross-state methodology research","Fiscal impact estimation","New code pricing analysis","Quality measure ↔ rate analysis","BLS wage adequacy calculations"].map((f: string, i: number) => (
                <div key={i}><span style={{ color:POS,marginRight:4 }}>✓</span>{f}</div>
              ))}
            </div>
            <div style={{ marginTop:10,fontSize:10,color:AL }}>
              Paid subscription coming soon.
            </div>
            <div style={{ marginTop:8 }}>
              <a href="#/pricing" style={{ fontSize:11,color:cO,fontWeight:600,textDecoration:"none" }}>Learn more about the AI tier &#8594;</a>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // ── Chat Screen ───────────────────────────────────────────────────────
  return (
    <div style={{ maxWidth:760,margin:"0 auto",padding:"10px 16px 0",fontFamily:"Helvetica Neue,Arial,sans-serif",color:A,display:"flex",flexDirection:"column",height:"calc(100vh - 120px)" }}>

      {/* Header */}
      <div style={{ paddingBottom:8,borderBottom:`1px solid ${BD}`,marginBottom:8,flexShrink:0 }}>
        <div style={{ display:"flex",justifyContent:"space-between",alignItems:"center" }}>
          <div style={{ display:"flex",alignItems:"center",gap:8 }}>
            <span style={{ fontSize:8,padding:"1px 6px",borderRadius:8,background:"rgba(196,89,10,0.1)",color:cO,fontWeight:600 }}>AI Tier</span>
            <span style={{ fontSize:9,color:AL,fontFamily:FM }}>Sonnet 4.5 · Tool-augmented · Aradune data</span>
          </div>
          <div style={{ display:"flex",gap:6,alignItems:"center" }}>
            {usage && <span style={{ fontSize:8,color:AL,fontFamily:FM }}>{usage.tool_rounds||0} lookups · {(usage.input_tokens||0)+(usage.output_tokens||0)} tokens</span>}
            <button onClick={()=>{setMessages([]);setUsage(null);}} style={{ fontSize:9,color:AL,background:SF,border:`1px solid ${BD}`,borderRadius:5,padding:"3px 8px",cursor:"pointer" }}>New Chat</button>
            <button onClick={()=>{setShowAuth(true);setToken("");setTokenState("");}} style={{ fontSize:9,color:NEG,background:"transparent",border:`1px solid ${BD}`,borderRadius:5,padding:"3px 8px",cursor:"pointer" }}>Sign Out</button>
          </div>
        </div>
      </div>

      {/* Messages */}
      <div style={{ flex:1,overflowY:"auto",paddingBottom:8 }}>
        {messages.length === 0 && !loading && (
          <div style={{ padding:"30px 0" }}>
            <div style={{ textAlign:"center",marginBottom:20 }}>
              <div style={{ fontSize:22,fontWeight:300,color:A,marginBottom:4 }}>Policy Analyst</div>
              <div style={{ fontSize:11,color:AL,maxWidth:400,margin:"0 auto",lineHeight:1.6 }}>
                Ask about Medicaid rates, fee schedule methodologies, fiscal impact, cross-state comparisons, or quality outcomes. Every answer is grounded in Aradune's dataset.
              </div>
            </div>
            <div style={{ display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(220px,1fr))",gap:8,maxWidth:560,margin:"0 auto" }}>
              {STARTERS.map((s,i) => (
                <button key={i} onClick={()=>sendMessage(s.q)}
                  style={{ textAlign:"left",padding:"10px 12px",background:WH,border:`1px solid ${BD}`,borderRadius:8,cursor:"pointer",transition:"all 0.15s",fontSize:11,color:A,lineHeight:1.4 }}
                  onMouseEnter={e=>e.currentTarget.style.borderColor=cB}
                  onMouseLeave={e=>e.currentTarget.style.borderColor=BD}>
                  <span style={{ marginRight:4 }}>{s.icon}</span> {s.q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} style={{ marginBottom:12,display:"flex",justifyContent:msg.role==="user"?"flex-end":"flex-start",alignItems:"flex-start",gap:8 }}>
            {msg.role === "assistant" && (
              <img src="/assets/icon-bot.png" alt="" style={{ width:32,height:32,borderRadius:"50%",objectFit:"cover",flexShrink:0,marginTop:2 }} />
            )}
            <div style={{
              maxWidth:"85%",
              padding:msg.role==="user"?"8px 14px":"12px 16px",
              borderRadius:msg.role==="user"?"12px 12px 2px 12px":"12px 12px 12px 2px",
              background:msg.role==="user"?cB:WH,
              color:msg.role==="user"?WH:A,
              fontSize:12,
              lineHeight:1.7,
              boxShadow:msg.role==="assistant"?SH:"none",
              border:msg.role==="assistant"?`1px solid ${BD}`:"none",
            }}>
              {msg.role === "assistant" ? renderMarkdown(msg.content) : msg.content}
            </div>
          </div>
        ))}

        {loading && (
          <div style={{ marginBottom:12,display:"flex",justifyContent:"flex-start" }}>
            <div style={{ padding:"12px 16px",borderRadius:"12px 12px 12px 2px",background:WH,boxShadow:SH,border:`1px solid ${BD}` }}>
              <div style={{ display:"flex",gap:4,alignItems:"center" }}>
                <div style={{ width:6,height:6,borderRadius:"50%",background:cB,opacity:0.4,animation:"pulse 1.2s ease-in-out infinite" }}/>
                <div style={{ width:6,height:6,borderRadius:"50%",background:cB,opacity:0.4,animation:"pulse 1.2s ease-in-out 0.2s infinite" }}/>
                <div style={{ width:6,height:6,borderRadius:"50%",background:cB,opacity:0.4,animation:"pulse 1.2s ease-in-out 0.4s infinite" }}/>
                <span style={{ fontSize:10,color:AL,marginLeft:6 }}>Analyzing...</span>
              </div>
            </div>
          </div>
        )}

        {error && !showAuth && (
          <div style={{ padding:"8px 12px",background:"rgba(164,38,44,0.06)",border:`1px solid rgba(164,38,44,0.15)`,borderRadius:8,fontSize:11,color:NEG,marginBottom:8 }}>{error}</div>
        )}

        <div ref={messagesEndRef}/>
      </div>

      {/* Input */}
      <div style={{ padding:"8px 0 12px",borderTop:`1px solid ${BD}`,flexShrink:0 }}>
        <div style={{ display:"flex",gap:8,alignItems:"flex-end" }}>
          <div style={{ flex:1,position:"relative" }}>
            <textarea
              ref={inputRef}
              value={input}
              onChange={e => setInput(e.currentTarget.value)}
              onKeyDown={e => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  sendMessage(input);
                }
              }}
              placeholder="Ask about rates, methodologies, fiscal impact..."
              rows={1}
              style={{
                width:"100%",padding:"10px 14px",border:`1px solid ${BD}`,borderRadius:10,fontSize:13,
                color:A,resize:"none",fontFamily:"Helvetica Neue,Arial,sans-serif",
                minHeight:42,maxHeight:140,boxSizing:"border-box",
                outline:"none",transition:"border-color 0.15s",
              }}
              onFocus={e => e.currentTarget.style.borderColor=cB}
              onBlur={e => e.currentTarget.style.borderColor=BD}
            />
          </div>
          <button
            onClick={() => sendMessage(input)}
            disabled={loading || !input.trim()}
            style={{
              padding:"10px 20px",background:loading||!input.trim()?"#ccc":cB,
              color:WH,border:"none",borderRadius:10,fontSize:12,fontWeight:600,
              cursor:loading||!input.trim()?"not-allowed":"pointer",
              flexShrink:0,height:42,
            }}>
            {loading ? "..." : "Send"}
          </button>
        </div>
        <div style={{ display:"flex",justifyContent:"space-between",marginTop:4 }}>
          <span style={{ fontSize:9,color:AL }}>Shift+Enter for new line · Powered by Claude Sonnet 4.5</span>
          <span style={{ fontSize:9,color:AL,fontFamily:FM }}>{usage ? `${usage.remaining ?? "?"} queries remaining this hour` : ""}</span>
        </div>
      </div>

      {/* CSS animation */}
      <style>{`@keyframes pulse { 0%,100% { opacity:0.3; } 50% { opacity:0.8; } }`}</style>
    </div>
  );
}
