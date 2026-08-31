/**
 * ShadowDashboard.jsx — Modal tableau de bord de l'ombrage sur la journée.
 *
 * Reçoit une série horaire {hour, alt, bldPct, canPct, totalPct} calculée pour
 * la zone (GeoJSON importé / ROI / vue) et affiche : synthèse, graphique en
 * courbes (% du sol à l'ombre au fil des heures) et tableau détaillé.
 */
import { useThemeContext } from "../theme";
import { F, M } from "../config";

const fmtArea = (m2) => m2 >= 1e6 ? `${(m2 / 1e6).toFixed(2)} km²` : m2 >= 1e4 ? `${(m2 / 1e4).toFixed(1)} ha` : `${Math.round(m2)} m²`;

export default function ShadowDashboard({ data = [], meta = {}, onClose }) {
  const C = useThemeContext();
  const day = data.filter((d) => !d.night);

  // ── graphique en courbes (SVG) ──
  const W = 560, Hc = 240, mL = 38, mR = 12, mT = 14, mB = 26;
  const iw = W - mL - mR, ih = Hc - mT - mB;
  const hours = day.map((d) => d.hour);
  const hMin = hours.length ? Math.min(...hours) : 6, hMax = hours.length ? Math.max(...hours) : 20;
  const X = (h) => mL + (hMax === hMin ? 0.5 : (h - hMin) / (hMax - hMin)) * iw;
  const Y = (p) => mT + (1 - Math.max(0, Math.min(100, p)) / 100) * ih;
  const line = (key) => day.map((d, i) => `${i ? "L" : "M"}${X(d.hour).toFixed(1)},${Y(d[key]).toFixed(1)}`).join(" ");
  const series = [
    { key: "totalPct", color: "#111827", label: "Total (bâti + arbres)" },
    { key: "bldPct", color: "#2563eb", label: "Bâtiments" },
    { key: "canPct", color: "#2e7d4f", label: "Canopée" },
  ];

  // repères clés
  const peak = day.reduce((a, b) => (b.totalPct > (a?.totalPct ?? -1) ? b : a), null);
  const low = day.reduce((a, b) => (b.totalPct < (a?.totalPct ?? 1e9) ? b : a), null);
  const mean = day.length ? day.reduce((s, d) => s + d.totalPct, 0) / day.length : 0;

  const th = { fontFamily: F, fontSize: 10.5, fontWeight: 600, color: C.dim, textAlign: "right", padding: "3px 8px", textTransform: "uppercase", letterSpacing: "0.04em", borderBottom: `1px solid ${C.bdr}` };
  const td = { fontFamily: M, fontSize: 11.5, color: C.txt, textAlign: "right", padding: "3px 8px", borderBottom: `0.5px solid ${C.bdr}` };
  const stat = (v, k, col) => (
    <div style={{ flex: 1, background: C.bg2 || C.bg, border: `0.5px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px" }}>
      <div style={{ fontFamily: M, fontSize: 17, fontWeight: 700, color: col || C.txt }}>{v}</div>
      <div style={{ fontFamily: F, fontSize: 10, color: C.dim, textTransform: "uppercase", letterSpacing: "0.04em" }}>{k}</div>
    </div>
  );

  return (
    <div onClick={onClose}
      style={{ position: "fixed", inset: 0, zIndex: 10000, background: "rgba(0,0,0,0.5)", display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }}>
      <div onClick={(e) => e.stopPropagation()}
        style={{ background: C.card || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 14, width: "min(760px, 96vw)", maxHeight: "88vh", overflow: "auto", boxShadow: "0 20px 60px rgba(0,0,0,0.35)" }}>
        {/* en-tête */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "14px 18px", borderBottom: `1px solid ${C.bdr}`, position: "sticky", top: 0, background: C.card || C.bg, zIndex: 1 }}>
          <div>
            <div style={{ fontFamily: F, fontSize: 15, fontWeight: 700, color: C.txt }}>Ombrage sur la journée</div>
            <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut }}>
              {meta.zoneName || "Zone"} · {meta.date} · surface {fmtArea(meta.zoneArea || 0)}{meta.dataset ? ` · ${meta.dataset}` : ""}
            </div>
          </div>
          <button onClick={onClose} style={{ fontFamily: F, fontSize: 18, lineHeight: 1, color: C.mut, background: "transparent", border: "none", cursor: "pointer", padding: 4 }}>×</button>
        </div>

        <div style={{ padding: 18, display: "flex", flexDirection: "column", gap: 16 }}>
          {/* synthèse */}
          <div style={{ display: "flex", gap: 8 }}>
            {stat(`${mean.toFixed(0)}%`, "Ombre moyenne (jour)")}
            {peak && stat(`${peak.totalPct.toFixed(0)}%`, `Max · ${peak.hour}h`, "#111827")}
            {low && stat(`${low.totalPct.toFixed(0)}%`, `Min · ${low.hour}h`)}
            {meta.canopy && stat(`${(meta.canPctStatic || 0).toFixed(0)}%`, "Couvert arboré", "#2e7d4f")}
          </div>

          {/* graphique */}
          <div>
            <div style={{ fontFamily: F, fontSize: 11.5, fontWeight: 600, color: C.txt, marginBottom: 4 }}>% du sol à l'ombre au fil de la journée</div>
            <div style={{ width: "100%", overflowX: "auto" }}>
              <svg viewBox={`0 0 ${W} ${Hc}`} style={{ width: "100%", minWidth: 420, height: "auto", display: "block" }}>
                {/* grille + axe Y */}
                {[0, 25, 50, 75, 100].map((p) => (
                  <g key={p}>
                    <line x1={mL} y1={Y(p)} x2={W - mR} y2={Y(p)} stroke={C.bdr} strokeWidth="0.5" />
                    <text x={mL - 6} y={Y(p) + 3} textAnchor="end" style={{ fontFamily: M, fontSize: 9, fill: C.dim }}>{p}</text>
                  </g>
                ))}
                {/* axe X (heures) */}
                {day.filter((_, i) => i % 2 === 0).map((d) => (
                  <text key={d.hour} x={X(d.hour)} y={Hc - 8} textAnchor="middle" style={{ fontFamily: M, fontSize: 9, fill: C.dim }}>{d.hour}h</text>
                ))}
                {/* courbes */}
                {series.map((sname) => (
                  <path key={sname.key} d={line(sname.key)} fill="none" stroke={sname.color} strokeWidth={sname.key === "totalPct" ? 2.2 : 1.4} strokeOpacity={sname.key === "totalPct" ? 1 : 0.85} />
                ))}
                {day.map((d) => <circle key={d.hour} cx={X(d.hour)} cy={Y(d.totalPct)} r="2" fill="#111827" />)}
              </svg>
            </div>
            {/* légende */}
            <div style={{ display: "flex", gap: 14, marginTop: 4, flexWrap: "wrap" }}>
              {series.map((sname) => (
                <span key={sname.key} style={{ display: "flex", alignItems: "center", gap: 5, fontFamily: F, fontSize: 10.5, color: C.mut }}>
                  <span style={{ width: 14, height: 3, background: sname.color, borderRadius: 2 }} /> {sname.label}
                </span>
              ))}
            </div>
          </div>

          {/* tableau */}
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={{ ...th, textAlign: "left" }}>Heure</th>
                  <th style={th}>Soleil</th>
                  <th style={th}>Bâtiments</th>
                  <th style={th}>Canopée</th>
                  <th style={th}>Total ombre</th>
                  <th style={th}>Aire ombragée</th>
                </tr>
              </thead>
              <tbody>
                {data.map((d) => (
                  <tr key={d.hour}>
                    <td style={{ ...td, textAlign: "left", fontWeight: 600 }}>{String(d.hour).padStart(2, "0")}:00</td>
                    <td style={td}>{d.night ? "—" : `${d.alt.toFixed(0)}°`}</td>
                    <td style={td}>{d.night ? "—" : `${d.bldPct.toFixed(0)}%`}</td>
                    <td style={{ ...td, color: "#2e7d4f" }}>{d.night ? "—" : `${d.canPct.toFixed(0)}%`}</td>
                    <td style={{ ...td, fontWeight: 700 }}>{d.night ? "nuit" : `${d.totalPct.toFixed(0)}%`}</td>
                    <td style={td}>{d.night ? "—" : fmtArea((d.totalPct / 100) * (meta.zoneArea || 0))}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div style={{ fontFamily: F, fontSize: 10, color: C.dim, lineHeight: 1.5 }}>
            % = fraction de la zone au sol dans l'ombre à chaque heure (temps solaire local, sol plat). « Total » = union bâtiments + canopée. La canopée utilise sa hauteur moyenne pour l'ombre.
          </div>
        </div>
      </div>
    </div>
  );
}
