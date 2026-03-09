import { C, FONT } from "../design";

export function LoadingBar({ text = "Loading data...", detail }: { text?: string; detail?: string }) {
  return (
    <div style={{ maxWidth: 480, margin: "0 auto", padding: "80px 20px", textAlign: "center", fontFamily: FONT.body }}>
      <div style={{ fontSize: 14, fontWeight: 600, color: C.ink, marginBottom: 16 }}>{text}</div>
      <div style={{
        width: "100%", height: 4, borderRadius: 2,
        background: C.border, overflow: "hidden",
      }}>
        <div style={{
          width: "40%", height: "100%", borderRadius: 2,
          background: C.brand,
          animation: "loadbar 1.2s ease-in-out infinite",
        }} />
      </div>
      {detail && <div style={{ fontSize: 11, color: C.inkLight, marginTop: 10 }}>{detail}</div>}
      <style>{`@keyframes loadbar { 0% { margin-left: -40%; } 100% { margin-left: 100%; } }`}</style>
    </div>
  );
}
