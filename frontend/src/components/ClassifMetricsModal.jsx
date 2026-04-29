/**
 * ClassifMetricsModal.jsx — Résultats de classification supervisée
 * Tabs : Résumé | Matrice de confusion | Importance des variables
 * Matrice : heatmap SVG (pure, sans D3 pour garder le bundle léger)
 */
import { useState, useRef, useEffect } from "react";
import { useThemeContext } from "../theme";
import { F } from "../config";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ── Helpers couleur ────────────────────────────────────────────────────────────
function lerp(a, b, t) { return a + (b-a)*t; }
function blueScale(t) {
  // blanc → bleu profond
  const r = Math.round(lerp(240,  25, t));
  const g = Math.round(lerp(248, 100, t));
  const bv= Math.round(lerp(255, 200, t));
  return `rgb(${r},${g},${bv})`;
}
function redScale(t) {
  // blanc → rouge
  const r = Math.round(lerp(255, 180, t));
  const g = Math.round(lerp(248,  20, t));
  const bv= Math.round(lerp(240,  20, t));
  return `rgb(${r},${g},${bv})`;
}

// ── Confusion Matrix SVG ───────────────────────────────────────────────────────
function ConfusionMatrix({ matrix, labels, C }) {
  const n = labels.length;
  if (!n || !matrix?.length) return (
    <p style={{color:C.dim, fontSize:11, textAlign:"center"}}>
      Matrice non disponible
    </p>
  );

  const CELL  = 56;
  const LEFT  = 100;
  const TOP   = 60;
  const W     = LEFT + n*CELL + 20;
  const H     = TOP  + n*CELL + 40;

  const maxVal = Math.max(...matrix.flat().map(Number)) || 1;

  return (
    <div style={{overflowX:"auto"}}>
      <svg width={W} height={H} style={{fontFamily:F}}>
        {/* Axe Y — Réel */}
        <text x={10} y={TOP + n*CELL/2} textAnchor="middle"
              fontSize={10} fill={C.dim}
              transform={`rotate(-90, 10, ${TOP + n*CELL/2})`}>
          Réel
        </text>
        {/* Axe X — Prédit */}
        <text x={LEFT + n*CELL/2} y={H-4} textAnchor="middle"
              fontSize={10} fill={C.dim}>
          Prédit
        </text>

        {labels.map((lbl, i) => (
          <g key={`row-${i}`}>
            {/* Label ligne */}
            <text x={LEFT-8} y={TOP + i*CELL + CELL/2 + 4}
                  textAnchor="end" fontSize={10} fill={C.txt}>
              {lbl.length > 12 ? lbl.slice(0,11)+"…" : lbl}
            </text>
            {labels.map((_, j) => {
              const val = Number(matrix[i]?.[j] ?? 0);
              const t   = maxVal > 0 ? val/maxVal : 0;
              const bg  = i===j ? blueScale(t) : (val>0 ? redScale(t*0.7) : "#f8f9fa");
              const fg  = t > 0.5 ? "#fff" : C.txt;
              return (
                <g key={`cell-${i}-${j}`}>
                  <rect x={LEFT+j*CELL} y={TOP+i*CELL}
                        width={CELL-2} height={CELL-2} rx={4}
                        fill={bg} stroke={C.bdr} strokeWidth={0.5}/>
                  <text x={LEFT+j*CELL+CELL/2} y={TOP+i*CELL+CELL/2+4}
                        textAnchor="middle" fontSize={11} fontWeight={i===j?600:400}
                        fill={fg}>
                    {val}
                  </text>
                </g>
              );
            })}
          </g>
        ))}

        {/* Labels colonnes */}
        {labels.map((lbl, j) => (
          <text key={`col-${j}`}
                x={LEFT+j*CELL+CELL/2} y={TOP-8}
                textAnchor="end" fontSize={10} fill={C.txt}
                transform={`rotate(-35, ${LEFT+j*CELL+CELL/2}, ${TOP-8})`}>
            {lbl.length > 12 ? lbl.slice(0,11)+"…" : lbl}
          </text>
        ))}

        {/* Légende */}
        <text x={LEFT} y={H-4} fontSize={9} fill={C.dim}>
          🟦 Diagonale = bonnes classifications   🟥 Hors diagonale = erreurs
        </text>
      </svg>
    </div>
  );
}

// ── Feature Importance ─────────────────────────────────────────────────────────
function FeatureImportance({ data, C }) {
  if (!data || !Object.keys(data).length) return (
    <p style={{color:C.dim, fontSize:11, textAlign:"center", padding:20}}>
      Importance non disponible pour ce modèle (SVM, KNN, Naïve Bayes)
    </p>
  );

  const items = Object.entries(data)
    .sort((a,b) => b[1]-a[1])
    .slice(0, 20);
  const max = items[0]?.[1] || 1;

  return (
    <div style={{display:"flex", flexDirection:"column", gap:6}}>
      {items.map(([name, val]) => (
        <div key={name} style={{display:"flex", alignItems:"center", gap:8}}>
          <span style={{fontSize:11, color:C.txt, width:80, flexShrink:0,
                        overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap"}}>
            {name}
          </span>
          <div style={{flex:1, height:18, background:C.hover, borderRadius:4, overflow:"hidden"}}>
            <div style={{
              width:`${(val/max)*100}%`, height:"100%",
              background:"#4A90D9", borderRadius:4,
              transition:"width .3s",
            }}/>
          </div>
          <span style={{fontSize:10, color:C.dim, width:44, textAlign:"right"}}>
            {(val*100).toFixed(1)}%
          </span>
        </div>
      ))}
    </div>
  );
}

// ── Modal principal ────────────────────────────────────────────────────────────
export default function ClassifMetricsModal({ result, onClose }) {
  const C = useThemeContext();
  const [tab, setTab] = useState("summary");

  if (!result) return null;

  const { metrics, confusion_matrix, class_labels, feature_importance, legend,
          backend, bands_used } = result;
  const { overall_accuracy, kappa, per_class } = metrics || {};

  const TAB_STYLE = (id) => ({
    padding: "7px 14px", fontSize: 11, border: "none", cursor: "pointer",
    fontFamily: F, borderRadius: "6px 6px 0 0",
    background: tab===id ? C.card : "transparent",
    color:      tab===id ? C.acc  : C.dim,
    fontWeight: tab===id ? 600    : 400,
  });

  const BADGE = (v, good) => {
    const pct = Math.round((v||0)*100);
    const bg  = pct >= 80 ? "#4daf4a22" : pct >= 60 ? "#ff7f0022" : "#e41a1c22";
    const fg  = pct >= 80 ? "#4daf4a"   : pct >= 60 ? "#e07b00"   : "#e41a1c";
    return (
      <div style={{background:bg, color:fg, borderRadius:8, padding:"4px 12px",
                   fontSize:22, fontWeight:700, textAlign:"center"}}>
        {pct}%
        <div style={{fontSize:10, fontWeight:400, marginTop:2}}>{good}</div>
      </div>
    );
  };

  return (
    <div style={{
      position:"fixed", inset:0, zIndex:9999,
      background:"rgba(0,0,0,.55)", display:"flex",
      alignItems:"center", justifyContent:"center",
    }}
      onClick={e => { if(e.target===e.currentTarget) onClose(); }}
    >
      <div style={{
        background:C.bg, borderRadius:14, padding:0, overflow:"hidden",
        width:"min(860px,95vw)", maxHeight:"88vh", display:"flex",
        flexDirection:"column", boxShadow:"0 20px 60px rgba(0,0,0,.4)",
        border:`1px solid ${C.bdr}`,
      }}>
        {/* Header */}
        <div style={{padding:"16px 20px 0", display:"flex",
                     alignItems:"center", justifyContent:"space-between"}}>
          <div>
            <h3 style={{margin:0, fontSize:14, fontFamily:F, color:C.txt}}>
              Résultats de classification
            </h3>
            <span style={{fontSize:10, color:C.dim}}>
              Backend : {backend === "gee" ? "GEE natif" : "sklearn local"}
              {bands_used?.length ? ` · ${bands_used.length} bande(s)` : ""}
            </span>
          </div>
          <button onClick={onClose} style={{
            background:"none", border:"none", cursor:"pointer",
            fontSize:18, color:C.dim, padding:4,
          }}>✕</button>
        </div>

        {/* Tabs */}
        <div style={{display:"flex", padding:"12px 20px 0", gap:2,
                     borderBottom:`1px solid ${C.bdr}`}}>
          <button style={TAB_STYLE("summary")}  onClick={()=>setTab("summary")}>
            📊 Résumé
          </button>
          <button style={TAB_STYLE("matrix")}   onClick={()=>setTab("matrix")}>
            🔢 Matrice de confusion
          </button>
          <button style={TAB_STYLE("importance")} onClick={()=>setTab("importance")}>
            📈 Importance
          </button>
        </div>

        {/* Body */}
        <div style={{flex:1, overflowY:"auto", padding:"20px"}}>

          {tab === "summary" && (
            <div style={{display:"flex", flexDirection:"column", gap:16}}>
              {/* Métriques globales */}
              <div style={{display:"flex", gap:12, justifyContent:"center"}}>
                {BADGE(overall_accuracy, "Accuracy globale")}
                {BADGE(kappa >= 0 ? (kappa+1)/2 : 0, `Kappa ${kappa?.toFixed(3)||"—"}`)}
              </div>

              {/* Légende */}
              <div style={{display:"flex", flexWrap:"wrap", gap:8}}>
                {(legend||[]).map(l => (
                  <div key={l.class_id} style={{display:"flex", alignItems:"center", gap:6,
                                                padding:"3px 10px", borderRadius:20,
                                                border:`1px solid ${C.bdr}`,
                                                fontSize:11, color:C.txt}}>
                    <span style={{width:10, height:10, borderRadius:"50%",
                                  background:l.color, flexShrink:0}}/>
                    {l.label}
                  </div>
                ))}
              </div>

              {/* Tableau per-class */}
              <div style={{overflowX:"auto"}}>
                <table style={{width:"100%", borderCollapse:"collapse",
                               fontFamily:F, fontSize:11}}>
                  <thead>
                    <tr style={{borderBottom:`1px solid ${C.bdr}`}}>
                      {["Classe","Précision","Rappel","F1","Support"].map(h => (
                        <th key={h} style={{padding:"6px 10px", textAlign:"left",
                                            color:C.dim, fontWeight:500}}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {(per_class||[]).map((row, i) => {
                      const lColor = legend?.[i]?.color || "#888";
                      return (
                        <tr key={row.class_id}
                            style={{borderBottom:`1px solid ${C.bdr}22`}}>
                          <td style={{padding:"8px 10px", display:"flex",
                                      alignItems:"center", gap:6}}>
                            <span style={{width:8, height:8, borderRadius:"50%",
                                          background:lColor, flexShrink:0}}/>
                            <strong style={{color:C.txt}}>{row.label}</strong>
                          </td>
                          {["precision","recall","f1"].map(k => (
                            <td key={k} style={{padding:"8px 10px", color:
                              row[k]>=0.8?"#4daf4a":row[k]>=0.6?"#e07b00":"#e41a1c"}}>
                              {Math.round((row[k]||0)*100)}%
                            </td>
                          ))}
                          <td style={{padding:"8px 10px", color:C.dim}}>
                            {row.support ?? "—"}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {tab === "matrix" && (
            <div style={{display:"flex", flexDirection:"column", gap:12}}>
              <p style={{fontSize:11, color:C.dim, margin:0}}>
                Lignes = classe réelle · Colonnes = classe prédite ·
                <strong style={{color:C.txt}}> Diagonale = bonnes classifications</strong>
              </p>
              <ConfusionMatrix
                matrix={confusion_matrix}
                labels={class_labels || []}
                C={C}
              />
            </div>
          )}

          {tab === "importance" && (
            <div style={{display:"flex", flexDirection:"column", gap:12}}>
              <p style={{fontSize:11, color:C.dim, margin:0}}>
                Contribution de chaque bande/indice à la classification
                (Random Forest · CART · Gradient Boosting uniquement)
              </p>
              <FeatureImportance data={feature_importance} C={C} />
            </div>
          )}
        </div>

        {/* Footer */}
        <div style={{padding:"12px 20px", borderTop:`1px solid ${C.bdr}`,
                     display:"flex", justifyContent:"flex-end"}}>
          <button onClick={onClose} style={{
            fontFamily:F, padding:"7px 20px", borderRadius:8, border:"none",
            background:C.acc, color:"#fff", cursor:"pointer", fontSize:11,
          }}>
            Fermer
          </button>
        </div>
      </div>
    </div>
  );
}
