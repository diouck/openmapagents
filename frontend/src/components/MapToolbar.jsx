import { useState } from "react";
import { getTheme } from "../theme";
import { F, M } from "../config";

const TOOL_GROUPS = [
  { label: "Selection", tools: [{ key: "pointer", label: "Selection", icon: "▷" }] },
  { label: "Mesure", tools: [
    { key: "measure_dist", label: "Distance", icon: "↔" },
    { key: "measure_area", label: "Surface", icon: "⬡" },
  ]},
  { label: "Dessin", tools: [
    { key: "buffer", label: "Buffer / zone tampon", icon: "◎" },
    { key: "draw", label: "Dessiner polygone", icon: "✎" },
  ]},
  { label: "Routing", tools: [
    { key: "route", label: "Itineraire", icon: "⤳" },
    { key: "isochrone", label: "Isochrone", icon: "◉" },
  ]},
  { label: "Export", tools: [
    { key: "print",    label: "Impression PDF",               icon: "⎙" },
    { key: "database", label: "Base de données",              icon: "🗄" },
    { key: "ogc",      label: "Services OGC (WMS/WFS/WMTS)",  icon: "📡" },
  ]},
];

const ALL_TOOLS = TOOL_GROUPS.flatMap(g => g.tools);

export default function MapToolbar({ activeTool, onTool, measureResult, bufferRadius, onBufferRadius, routeProfile, onRouteProfile, isoTime, onIsoTime }) {
  const C = getTheme();
  const [open, setOpen] = useState(false);
  const activeDef = ALL_TOOLS.find(t => t.key === activeTool);

  return (
    <div style={{ position: "absolute", top: 10, left: 10, zIndex: 20 }}>
      <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
        {/* Menu button */}
        <button onClick={() => setOpen(o => !o)} style={{
          fontFamily: F, fontSize: 12, padding: "6px 12px", borderRadius: 8,
          background: C.card + "ee", border: `0.5px solid ${open ? C.acc + "55" : C.bdr}`,
          color: open ? C.acc : C.txt, cursor: "pointer", backdropFilter: "blur(8px)",
          display: "flex", alignItems: "center", gap: 6,
        }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M14.7 6.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94l-3.76 3.76z" />
          </svg>
          Outils
        </button>

        {/* Active tool pill */}
        {activeTool !== "pointer" && activeDef && (
          <div style={{
            fontFamily: F, fontSize: 11, padding: "4px 10px", borderRadius: 6,
            background: C.acc + "18", color: C.acc, border: `0.5px solid ${C.acc}44`,
            backdropFilter: "blur(8px)",
            display: "flex", alignItems: "center", gap: 4,
          }}>
            <span>{activeDef.icon}</span>
            <span>{activeDef.label}</span>
            <button onClick={() => onTool("pointer")} style={{
              fontSize: 10, background: "none", border: "none", color: C.acc, cursor: "pointer", padding: "0 0 0 4px", fontFamily: F,
            }}>✕</button>
          </div>
        )}

        {/* Buffer params */}
        {activeTool === "buffer" && (
          <div style={{ display: "flex", alignItems: "center", gap: 4, padding: "4px 8px", background: C.card + "ee", borderRadius: 6, border: `0.5px solid ${C.bdr}`, backdropFilter: "blur(8px)" }}>
            <input type="range" min="100" max="5000" step="100" value={bufferRadius}
              onChange={e => onBufferRadius(parseInt(e.target.value))} style={{ width: 70, height: 3 }} />
            <span style={{ fontSize: 10, color: C.acc, fontFamily: M }}>
              {bufferRadius >= 1000 ? `${bufferRadius / 1000}km` : `${bufferRadius}m`}
            </span>
          </div>
        )}

        {/* Measure result */}
        {measureResult && (
          <div style={{ padding: "4px 10px", fontSize: 11, color: C.amb, fontFamily: M, background: C.card + "ee", borderRadius: 6, border: `0.5px solid ${C.bdr}`, backdropFilter: "blur(8px)" }}>
            {measureResult}
          </div>
        )}
      </div>

      {/* Dropdown menu */}
      {open && (
        <div style={{
          position: "absolute", top: "100%", left: 0, marginTop: 4,
          background: C.card, borderRadius: 8, border: `0.5px solid ${C.bdr}`,
          boxShadow: "0 4px 16px rgba(0,0,0,0.25)", minWidth: 210, overflow: "hidden",
        }}>
          {TOOL_GROUPS.map(group => (
            <div key={group.label}>
              <div style={{ fontSize: 10, color: C.dim, padding: "6px 12px 2px", textTransform: "uppercase", letterSpacing: "0.05em" }}>
                {group.label}
              </div>
              {group.tools.map(t => (
                <button key={t.key} onClick={() => { onTool(t.key); setOpen(false); }}
                  style={{
                    fontFamily: F, fontSize: 12, padding: "7px 12px", width: "100%",
                    display: "flex", alignItems: "center", gap: 8, border: "none", cursor: "pointer",
                    background: activeTool === t.key ? C.acc + "12" : "transparent",
                    color: activeTool === t.key ? C.acc : C.txt, textAlign: "left",
                  }}
                  onMouseEnter={e => { if (activeTool !== t.key) e.currentTarget.style.background = C.hover; }}
                  onMouseLeave={e => { e.currentTarget.style.background = activeTool === t.key ? C.acc + "12" : "transparent"; }}>
                  <span style={{ fontSize: 14, width: 20, textAlign: "center" }}>{t.icon}</span>
                  <span>{t.label}</span>
                  {activeTool === t.key && <span style={{ marginLeft: "auto", fontSize: 10, color: C.acc }}>actif</span>}
                </button>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
