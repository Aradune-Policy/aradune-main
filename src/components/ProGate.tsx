import { useState, useCallback } from "react";
import { C, FONT, SHADOW_LG } from "../design";

// ── Pro Access Hook ──────────────────────────────────────────────────
export function useProAccess(): { isPro: boolean } {
  try {
    const token = localStorage.getItem("aradune_token");
    return { isPro: !!token };
  } catch {
    return { isPro: false };
  }
}

// ── Pro Badge ────────────────────────────────────────────────────────
export function ProBadge() {
  return (
    <span style={{
      display: "inline-flex", alignItems: "center",
      fontSize: 8, fontFamily: FONT.mono, fontWeight: 700,
      color: C.accent, background: `${C.accent}12`,
      padding: "1px 5px", borderRadius: 4, letterSpacing: 0.5,
      marginLeft: 4, verticalAlign: "middle",
    }}>PRO</span>
  );
}

// ── Pro Gate Modal ───────────────────────────────────────────────────
export function ProGateModal({
  feature,
  open,
  onClose,
}: {
  feature: string;
  open: boolean;
  onClose: () => void;
}) {
  const [tokenInput, setTokenInput] = useState("");
  const [showTokenField, setShowTokenField] = useState(false);

  const handleSaveToken = useCallback(() => {
    const t = tokenInput.trim();
    if (!t) return;
    try { localStorage.setItem("aradune_token", t); } catch {}
    setTokenInput("");
    setShowTokenField(false);
    onClose();
    window.location.reload();
  }, [tokenInput, onClose]);

  if (!open) return null;

  return (
    <div onClick={onClose} style={{
      position: "fixed", inset: 0, zIndex: 9999,
      background: "rgba(10,37,64,0.45)", backdropFilter: "blur(4px)",
      display: "flex", alignItems: "center", justifyContent: "center",
    }}>
      <div onClick={e => e.stopPropagation()} style={{
        background: C.white, borderRadius: 14, boxShadow: SHADOW_LG,
        padding: "32px 28px 24px", maxWidth: 400, width: "90%",
      }}>
        <div style={{
          fontSize: 9, fontFamily: FONT.mono, fontWeight: 700,
          color: C.accent, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8,
        }}>Professional Feature</div>

        <div style={{ fontSize: 16, fontWeight: 700, color: C.ink, marginBottom: 6 }}>
          {feature}
        </div>

        <div style={{ fontSize: 12, color: C.inkLight, lineHeight: 1.7, marginBottom: 16 }}>
          This feature is part of the Professional tier. Pro subscribers get branded
          PDF reports, formatted Excel workbooks, persistent saved scenarios, batch
          HCPCS lookups, and the AI Policy Analyst.
        </div>

        <div style={{ display: "grid", gap: 8, marginBottom: 16 }}>
          <a href="#/pricing" onClick={onClose} style={{
            display: "block", textAlign: "center",
            padding: "10px 20px", background: C.accent, color: C.white,
            borderRadius: 8, fontSize: 12, fontWeight: 600, textDecoration: "none",
          }}>
            View Professional Tier
          </a>
        </div>

        {!showTokenField ? (
          <button onClick={() => setShowTokenField(true)} style={{
            background: "none", border: "none", color: C.inkLight,
            fontSize: 11, cursor: "pointer", padding: 0, textDecoration: "underline",
          }}>
            I have a token
          </button>
        ) : (
          <div style={{ display: "flex", gap: 6 }}>
            <input
              value={tokenInput}
              onChange={e => setTokenInput(e.target.value)}
              placeholder="Paste your token"
              style={{
                flex: 1, padding: "7px 10px", borderRadius: 6,
                border: `1px solid ${C.border}`, fontSize: 11,
                fontFamily: FONT.mono, outline: "none",
              }}
              onKeyDown={e => { if (e.key === "Enter") handleSaveToken(); }}
            />
            <button onClick={handleSaveToken} style={{
              padding: "7px 14px", background: C.ink, color: C.white,
              border: "none", borderRadius: 6, fontSize: 11,
              fontWeight: 600, cursor: "pointer",
            }}>
              Activate
            </button>
          </div>
        )}

        <div style={{ marginTop: 12, textAlign: "right" }}>
          <button onClick={onClose} style={{
            background: "none", border: "none", color: C.inkLight,
            fontSize: 11, cursor: "pointer",
          }}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
