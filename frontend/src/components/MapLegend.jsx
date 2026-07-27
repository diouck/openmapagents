/**
 * MapLegend.jsx — Overlay légende thématique flottant sur la carte
 * Déclenché par le LLM via thematic_analysis → handleToolResult
 *
 * Props :
 *   legend  : {
 *     title      : string,
 *     type       : "graduated" | "categorized" | "proportional",
 *     classResult: { classes, entries, minVal, maxVal, minSize, maxSize } (de buildClassification),
 *     vals?      : number[]   (pour proportional)
 *   }
 *   onClose : () => void
 */

import { useState } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcX, IcChevronUp, IcChevronDown } from "../icons";

// ── Formatage surface ────────────────────────────────────────────
function fmtArea(ha) {
  if (!ha || ha === 0) return null;
  if (ha < 1)   return `${Math.round(ha * 10000)} m²`;
  if (ha < 100) return `${ha.toFixed(1)} ha`;
  return `${(ha / 100).toFixed(2)} km²`;
}

// ── Formater une valeur numérique ─────────────────────────────
function fmt(v) {
  if (v == null || isNaN(v)) return "?";
  if (Math.abs(v) >= 1e9) return (v / 1e9).toFixed(1) + " Md";
  if (Math.abs(v) >= 1e6) return (v / 1e6).toFixed(1) + " M";
  if (Math.abs(v) >= 1e3) return (v / 1e3).toFixed(1) + " k";
  return Number.isInteger(v) ? v.toString() : v.toFixed(2);
}

// ── Légende graduated (choroplèthe) ──────────────────────────
function GraduatedLegend({ classResult, title }) {
  const C = useThemeContext();
  const classes = classResult?.classes || [];
  if (!classes.length) return null;
  return (
    <>
      <div style={{ fontSize: 10, color: C.dim, marginBottom: 5,
                    textTransform: "uppercase", letterSpacing: ".05em" }}>
        {title}
      </div>
      {classes.map((cls, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 3 }}>
          <div style={{ width: 16, height: 10, borderRadius: 2,
                        background: cls.color, flexShrink: 0 }} />
          <span style={{ fontSize: 10, color: C.txt }}>
            {fmt(cls.min)} – {fmt(cls.max)}
          </span>
          {cls.count != null && (
            <span style={{ fontSize: 9, color: C.dim, marginLeft: "auto" }}>
              {cls.count}
            </span>
          )}
        </div>
      ))}
    </>
  );
}

// ── Légende categorized ───────────────────────────────────────
function CategorizedLegend({ classResult, title }) {
  const C = useThemeContext();
  const entries = classResult?.entries || [];
  if (!entries.length) return null;
  return (
    <>
      <div style={{ fontSize: 10, color: C.dim, marginBottom: 5,
                    textTransform: "uppercase", letterSpacing: ".05em" }}>
        {title}
      </div>
      {entries.map((e, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 3 }}>
          <div style={{ width: 12, height: 12, borderRadius: "50%",
                        background: e.color, flexShrink: 0 }} />
          <span style={{ fontSize: 10, color: C.txt,
                         maxWidth: 140, overflow: "hidden",
                         textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            {e.value}
          </span>
          {e.count != null && (
            <span style={{ fontSize: 9, color: C.dim, marginLeft: "auto" }}>
              {e.count}
            </span>
          )}
        </div>
      ))}
    </>
  );
}

// ── Légende gradient continu (raster GEE) ───────────────────
function GeeGradientLegend({ visParams }) {
  const C = useThemeContext();
  if (!visParams?.palette?.length) return null;

  const palette  = visParams.palette.map(c => c.startsWith("#") ? c : `#${c}`);
  const gradient = `linear-gradient(to right, ${palette.join(", ")})`;
  const min = visParams.min ?? 0;
  const max = visParams.max ?? 1;
  const mid = (min + max) / 2;
  const unit = visParams.unit || "";

  const fmt = v => {
    if (Math.abs(v) >= 1000) return v.toFixed(0);
    if (Math.abs(v) >= 1)    return v.toFixed(1);
    return v.toFixed(2);
  };

  return (
    <div>
      <div style={{
        height: 10, borderRadius: 5,
        background: gradient,
        margin: "4px 0 3px 0",
        boxShadow: "0 1px 3px rgba(0,0,0,0.3)",
      }} />
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "center",
        fontSize: 9, color: C.dim, fontFamily: M,
      }}>
        <span>{fmt(min)}</span>
        {unit && <span style={{ color: C.acc, fontWeight: 600, fontSize: 10 }}>{unit}</span>}
        <span>{fmt(max)}</span>
      </div>
    </div>
  );
}


// ── Légende proportional ──────────────────────────────────────
function ProportionalLegend({ legend, title }) {
  const C    = useThemeContext();
  const vals = legend.vals || [];
  if (!vals.length) return null;

  const minVal  = Math.min(...vals);
  const maxVal  = Math.max(...vals);
  const minSize = legend.minSize || 3;
  const maxSize = legend.maxSize || 40;

  // 3 niveaux de référence : min, médiane, max
  const med = vals.sort((a, b) => a - b)[Math.floor(vals.length / 2)];
  const levels = [
    { val: minVal, size: minSize },
    { val: med,    size: minSize + (maxSize - minSize) * 0.5 },
    { val: maxVal, size: maxSize },
  ].filter((l, i, arr) => i === 0 || l.val !== arr[i-1].val);

  return (
    <>
      <div style={{ fontSize: 10, color: C.dim, marginBottom: 8,
                    textTransform: "uppercase", letterSpacing: ".05em" }}>
        {title}
      </div>
      <div style={{ display: "flex", alignItems: "flex-end", gap: 12, justifyContent: "center" }}>
        {levels.map((l, i) => (
          <div key={i} style={{ display: "flex", flexDirection: "column",
                                alignItems: "center", gap: 4 }}>
            <div style={{
              width:        l.size * 2,
              height:       l.size * 2,
              borderRadius: "50%",
              background:   C.acc + "88",
              border:       `1.5px solid ${C.acc}`,
              flexShrink:   0,
            }} />
            <span style={{ fontSize: 9, color: C.dim, fontFamily: M }}>
              {fmt(l.val)}
            </span>
          </div>
        ))}
      </div>
    </>
  );
}

// ── Overlay légende classification raster ─────────────────────
/**
 * ClassifMapLegend — affiché automatiquement sur la carte pour
 * toutes les couches raster visibles ayant une légende de classification.
 * Positionné en bas à droite, collapsable, multi-couche.
 */
export function ClassifMapLegend({ layers }) {
  const C = useThemeContext();
  const [minimized,  setMinimized]  = useState(false);
  const [activeIdx,  setActiveIdx]  = useState(0);

  const eligible = (layers || []).filter(l =>
    l.visible && l.isRaster && l.legend?.length > 0
  );
  if (!eligible.length) return null;

  const safeIdx = Math.min(activeIdx, eligible.length - 1);
  const layer   = eligible[safeIdx];
  const entries = layer.legend || [];

  return (
    <div style={{
      position: "absolute", bottom: 32, right: 10, zIndex: 900,
      background: C.card, border: `1px solid ${C.bdr}`, borderRadius: 8,
      boxShadow: "0 4px 16px rgba(0,0,0,.3)",
      minWidth: 160, maxWidth: 230, fontFamily: F,
    }}>
      {/* En-tête cliquable */}
      <div
        onClick={() => setMinimized(v => !v)}
        style={{
          display: "flex", alignItems: "center", gap: 6,
          padding: "6px 10px",
          borderBottom: minimized ? "none" : `0.5px solid ${C.bdr}`,
          cursor: "pointer", background: C.hover,
          borderRadius: minimized ? 8 : "8px 8px 0 0",
        }}
      >
        {/* Pastilles couleur compactes */}
        <div style={{ display:"flex", gap:2, flexShrink:0 }}>
          {entries.slice(0,5).map(e => (
            <div key={e.class_id} style={{
              width:8, height:8, borderRadius:2, background:e.color,
              border:"0.5px solid rgba(0,0,0,.15)",
            }}/>
          ))}
          {entries.length > 5 && (
            <span style={{ fontSize:8, color:C.dim, lineHeight:"8px" }}>
              +{entries.length - 5}
            </span>
          )}
        </div>
        <span style={{
          fontSize:10, fontWeight:600, color:C.txt, flex:1,
          overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap",
        }}>
          {layer.name}
        </span>
        <span style={{ fontSize:11, color:C.dim, flexShrink:0 }}>
          {minimized ? "▲" : "▼"}
        </span>
      </div>

      {!minimized && (
        <>
          {/* Sélecteur de couche si plusieurs */}
          {eligible.length > 1 && (
            <div style={{
              display:"flex", gap:3, padding:"4px 8px",
              borderBottom:`0.5px solid ${C.bdr}`, flexWrap:"wrap",
            }}>
              {eligible.map((l, i) => (
                <button key={l.id} onClick={() => setActiveIdx(i)} style={{
                  fontFamily:F, fontSize:9, padding:"2px 6px",
                  borderRadius:4, cursor:"pointer",
                  background: i === safeIdx ? C.acc+"22" : "transparent",
                  color:      i === safeIdx ? C.acc      : C.dim,
                  border:     `0.5px solid ${i === safeIdx ? C.acc : C.bdr}`,
                }}>
                  {i + 1}
                </button>
              ))}
            </div>
          )}

          {/* Entrées de légende */}
          <div style={{
            padding:"8px 10px 10px",
            overflowY:"auto", maxHeight:320,
            display:"flex", flexDirection:"column", gap:4,
          }}>
            {entries.map(e => (
              <div key={e.class_id}
                   style={{ display:"flex", alignItems:"center", gap:7 }}>
                <div style={{
                  width:12, height:12, borderRadius:2, flexShrink:0,
                  background:e.color, border:"0.5px solid rgba(0,0,0,.12)",
                }}/>
                <span style={{
                  fontSize:10, color:C.txt, flex:1,
                  overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap",
                }}>
                  {e.label}
                </span>
                {fmtArea(e.area_ha) && (
                  <span style={{
                    fontSize:9, color:C.dim, fontFamily:M,
                    flexShrink:0, whiteSpace:"nowrap",
                  }}>
                    {fmtArea(e.area_ha)}
                  </span>
                )}
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

// ── Composant principal ───────────────────────────────────────
export default function MapLegend({ legend, onClose }) {
  const C = useThemeContext();
  const [minimized, setMinimized] = useState(false);

  if (!legend) return null;

  const { title, type, classResult } = legend;

  return (
    <div style={{
      position:     "absolute",
      bottom:       80,
      left:         12,
      zIndex:       900,
      background:   C.card,
      border:       `1px solid ${C.bdr}`,
      borderRadius: 8,
      boxShadow:    "0 4px 16px rgba(0,0,0,0.25)",
      minWidth:     180,
      maxWidth:     220,
      maxHeight:    420,
      overflow:     "hidden",
      fontFamily:   F,
    }}>
      {/* Header */}
      <div style={{
        display:        "flex",
        alignItems:     "center",
        justifyContent: "space-between",
        padding:        "7px 10px",
        borderBottom:   minimized ? "none" : `0.5px solid ${C.bdr}`,
        cursor:         "pointer",
      }} onClick={() => setMinimized(v => !v)}>
        <div style={{ fontSize: 11, fontWeight: 600, color: C.txt,
                      overflow: "hidden", textOverflow: "ellipsis",
                      whiteSpace: "nowrap", maxWidth: 160 }}>
          {title || "Légende"}
        </div>
        <div style={{ display: "flex", gap: 4, flexShrink: 0 }}>
          <button
            onClick={e => { e.stopPropagation(); setMinimized(v => !v); }}
            style={{ background: "none", border: "none", color: C.dim,
                     cursor: "pointer", display: "flex", padding: "0 2px" }}>
            {minimized ? <IcChevronUp size={14}/> : <IcChevronDown size={14}/>}
          </button>
          <button
            onClick={e => { e.stopPropagation(); onClose(); }}
            style={{ background: "none", border: "none", color: C.dim,
                     cursor: "pointer", display: "flex", padding: "0 2px" }}>
            <IcX size={14}/>
          </button>
        </div>
      </div>

      {/* Contenu */}
      {!minimized && (
        <div style={{ padding: "8px 10px 10px", overflowY: "auto", maxHeight: 360 }}>
          {type === "graduated" && (
            <GraduatedLegend classResult={classResult} title="" />
          )}
          {type === "categorized" && (
            <CategorizedLegend classResult={classResult} title="" />
          )}
          {type === "proportional" && (
            <ProportionalLegend legend={legend} title="" />
          )}
          {/* Raster GEE — gradient continu */}
          {type === "gee_raster" && legend.visParams && (
            <GeeGradientLegend visParams={legend.visParams} />
          )}
          {/* Fallback si classResult absent */}
          {!classResult && type !== "proportional" && type !== "gee_raster" && (
            <div style={{ fontSize: 11, color: C.dim, textAlign: "center", padding: 8 }}>
              Légende non disponible
            </div>
          )}
        </div>
      )}
    </div>
  );
}
