/**
 * TimeSeriesModal.jsx — Graphique de série temporelle d'un indice GEE.
 * Reçoit series = [{ date:"YYYY-MM", value:Number|null }] (mensuel).
 * Rendu SVG autonome (ligne + points + axes) + résumé + export CSV.
 */
import { useState } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcTrendingUp, IcFileDown, IcX } from "../icons";

export default function TimeSeriesModal({ dataset, index, series = [], agg = "monthly", onClose }) {
  const C = useThemeContext();
  const [hover, setHover] = useState(null);
  const aggLabel = agg === "yearly" ? "annuelle" : "mensuelle";

  const pts = series.filter(s => s && s.value != null && isFinite(s.value));
  const W = 620, H = 300, PADL = 48, PADR = 16, PADT = 20, PADB = 40;
  const iw = W - PADL - PADR, ih = H - PADT - PADB;

  let body;
  if (pts.length < 2) {
    body = <div style={{ padding: 30, textAlign: "center", color: C.dim, fontSize: 12 }}>
      Pas assez de données valides sur la période/zone (nuages ? période trop courte ?).
    </div>;
  } else {
    const vals = pts.map(p => p.value);
    let vmin = Math.min(...vals), vmax = Math.max(...vals);
    if (vmin === vmax) { vmin -= 1; vmax += 1; }
    const pad = (vmax - vmin) * 0.08; vmin -= pad; vmax += pad;
    const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
    const X = i => PADL + (i / (pts.length - 1)) * iw;
    const Y = v => PADT + (1 - (v - vmin) / (vmax - vmin)) * ih;
    const line = pts.map((p, i) => `${X(i).toFixed(1)},${Y(p.value).toFixed(1)}`).join(" ");
    const yTicks = 5;
    const trend = pts[pts.length - 1].value - pts[0].value;
    // labels de date : ~6 max
    const step = Math.max(1, Math.round(pts.length / 6));

    body = (
      <>
        <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto", display: "block" }}
          onMouseLeave={() => setHover(null)}>
          {/* grille + axe Y */}
          {Array.from({ length: yTicks + 1 }).map((_, i) => {
            const v = vmin + (i / yTicks) * (vmax - vmin);
            const y = Y(v);
            return (
              <g key={i}>
                <line x1={PADL} y1={y} x2={W - PADR} y2={y} stroke={C.bdr} strokeWidth="0.5" />
                <text x={PADL - 6} y={y + 3} textAnchor="end" fontSize="9" fill={C.dim} fontFamily={M}>{v.toFixed(2)}</text>
              </g>
            );
          })}
          {/* moyenne */}
          <line x1={PADL} y1={Y(mean)} x2={W - PADR} y2={Y(mean)} stroke={C.amb} strokeWidth="1" strokeDasharray="4 3" opacity="0.7" />
          {/* labels X */}
          {pts.map((p, i) => (i % step === 0 || i === pts.length - 1) && (
            <text key={i} x={X(i)} y={H - PADB + 14} textAnchor="middle" fontSize="8" fill={C.dim} fontFamily={M}>{p.date}</text>
          ))}
          {/* courbe */}
          <polyline points={line} fill="none" stroke={C.acc} strokeWidth="2" />
          {/* points + zones de survol */}
          {pts.map((p, i) => (
            <g key={i}>
              <circle cx={X(i)} cy={Y(p.value)} r={hover === i ? 4 : 2.5} fill={C.acc} />
              <rect x={X(i) - iw / (pts.length * 2)} y={PADT} width={Math.max(4, iw / pts.length)} height={ih}
                fill="transparent" onMouseEnter={() => setHover(i)} />
            </g>
          ))}
          {/* tooltip */}
          {hover != null && pts[hover] && (
            <g>
              <line x1={X(hover)} y1={PADT} x2={X(hover)} y2={PADT + ih} stroke={C.acc} strokeWidth="0.5" opacity="0.5" />
              <text x={Math.min(Math.max(X(hover), PADL + 30), W - PADR - 30)} y={PADT + 10} textAnchor="middle" fontSize="10" fill={C.txt} fontFamily={M} fontWeight="600">
                {pts[hover].date} : {pts[hover].value.toFixed(3)}
              </text>
            </g>
          )}
        </svg>

        {/* Résumé */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 6, marginTop: 10 }}>
          {[
            ["Points", pts.length],
            ["Min", Math.min(...vals).toFixed(3)],
            ["Max", Math.max(...vals).toFixed(3)],
            ["Moyenne", mean.toFixed(3)],
            ["Début", pts[0].value.toFixed(3)],
            ["Fin", pts[pts.length - 1].value.toFixed(3)],
            ["Tendance", (trend >= 0 ? "▲ +" : "▼ ") + trend.toFixed(3)],
            ["Période", `${pts[0].date} → ${pts[pts.length - 1].date}`],
          ].map(([k, v]) => (
            <div key={k} style={{ background: C.input, borderRadius: 5, padding: "4px 7px", fontSize: 9 }}>
              <div style={{ color: C.dim }}>{k}</div>
              <div style={{ color: k === "Tendance" ? (trend >= 0 ? C.acc : C.red) : C.txt, fontFamily: M, fontSize: 10 }}>{v}</div>
            </div>
          ))}
        </div>
      </>
    );
  }

  const exportCsv = () => {
    const rows = ["date,value", ...series.map(s => `${s.date},${s.value == null ? "" : s.value}`)].join("\n");
    const blob = new Blob([rows], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = `serie_${index}.csv`; a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  };

  return (
    <div onClick={onClose} style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,.45)", zIndex: 10000, display: "flex", alignItems: "center", justifyContent: "center", padding: 20 }}>
      <div onClick={e => e.stopPropagation()} style={{ background: C.bg, borderRadius: 12, border: `0.5px solid ${C.bdr}`, width: 680, maxWidth: "95vw", maxHeight: "90vh", overflowY: "auto", padding: 16, boxShadow: "0 12px 40px rgba(0,0,0,.4)" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
          <span style={{ display: "flex", color: C.acc }}><IcTrendingUp size={16}/></span>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 13, fontWeight: 600, color: C.txt }}>Série temporelle — {index}</div>
            <div style={{ fontSize: 9, color: C.dim }}>{dataset} · valeur moyenne {aggLabel} sur la zone</div>
          </div>
          {pts.length >= 1 && (
            <button onClick={exportCsv} style={{ fontFamily: F, fontSize: 9, padding: "4px 8px", borderRadius: 5, background: "transparent", border: `0.5px solid ${C.acc}66`, color: C.acc, cursor: "pointer", display: "flex", alignItems: "center", gap: 4 }}><IcFileDown size={12}/> CSV</button>
          )}
          <button onClick={onClose} style={{ background: "transparent", border: "none", color: C.dim, cursor: "pointer", display: "flex", padding: 2 }}><IcX size={16}/></button>
        </div>
        {body}
      </div>
    </div>
  );
}
