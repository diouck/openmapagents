import { getTheme } from "../theme";
import { M } from "../config";

// ── Cercles superposés SVG — style SIG ────────────────────────
function NestedCircles({ cr, color }) {
  const maxR   = Math.min(28, Math.max(6, cr.maxSize));
  const scale  = maxR / cr.maxSize;
  const medVal = Math.round((cr.minVal + cr.maxVal) / 2);
  const medR   = (cr.minSize + cr.maxSize) / 2;
  const W      = maxR * 2 + 60; // largeur SVG : cercles + labels
  const H      = maxR * 2 + 4;
  const cx     = maxR + 1;      // centre X commun
  const base   = H - 1;         // ligne de base commune

  const entries = [
    { r: cr.maxSize, val: cr.maxVal },
    { r: medR,       val: medVal   },
    { r: cr.minSize, val: cr.minVal},
  ];

  return (
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} style={{ display: "block", overflow: "visible" }}>
      {/* Du plus grand au plus petit pour la superposition */}
      {entries.map(({ r, val }, i) => {
        const dr = Math.max(1.5, r * scale);
        const cy = base - dr;
        return (
          <g key={i}>
            {/* Cercle rempli */}
            <circle cx={cx} cy={cy} r={dr} fill={color} opacity="0.85" />
            {/* Contour blanc pour distinguer */}
            <circle cx={cx} cy={cy} r={dr} fill="none" stroke="rgba(255,255,255,0.5)" strokeWidth="1" />
            {/* Trait de cote en pointillés */}
            <line
              x1={cx + dr} y1={cy}
              x2={maxR * 2 + 6} y2={cy}
              stroke="rgba(255,255,255,0.45)" strokeWidth="0.8"
              strokeDasharray="2,2"
            />
            {/* Label */}
            <text
              x={maxR * 2 + 8} y={cy + 3.5}
              fontSize="9" fill="rgba(220,220,220,0.9)"
              fontFamily="sans-serif"
            >{val?.toLocaleString("fr")}</text>
          </g>
        );
      })}
    </svg>
  );
}

// ── Lignes superposées SVG ─────────────────────────────────────
function NestedLines({ cr, color }) {
  const maxW  = Math.min(12, Math.max(1, cr.maxSize));
  const scale = maxW / cr.maxSize;
  const medVal = Math.round((cr.minVal + cr.maxVal) / 2);
  const medW   = (cr.minSize + cr.maxSize) / 2;
  const lineLen = 28;
  const W = lineLen + 60;

  const entries = [
    { w: cr.maxSize, val: cr.maxVal },
    { w: medW,       val: medVal   },
    { w: cr.minSize, val: cr.minVal},
  ];
  const rowH = 14;
  const H = entries.length * rowH + 4;

  return (
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} style={{ display: "block" }}>
      {entries.map(({ w, val }, i) => {
        const dw = Math.max(0.5, w * scale);
        const y  = i * rowH + rowH / 2 + 2;
        return (
          <g key={i}>
            <line x1="2" y1={y} x2={lineLen} y2={y}
              stroke={color} strokeWidth={dw} strokeLinecap="round" opacity="0.9" />
            <text x={lineLen + 5} y={y + 3.5}
              fontSize="9" fill="rgba(220,220,220,0.9)"
              fontFamily="sans-serif"
            >{val?.toLocaleString("fr")}</text>
          </g>
        );
      })}
    </svg>
  );
}

// ── Légende principale ─────────────────────────────────────────
export default function Legend({ layers }) {
  const C = getTheme();
  const visible = layers.filter(l => l.visible);
  if (!visible.length) return null;

  return (
    <div style={{
      position: "absolute", bottom: 30, left: 10, zIndex: 10, maxWidth: 220,
      borderRadius: 8, padding: 10, maxHeight: "40vh", overflowY: "auto",
    }}>
      {visible.map(layer => {
        const cr = layer.classResult;
        return (
          <div key={layer.id} style={{ marginBottom: 8 }}>

            {/* Nom couche + pastille */}
            <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: cr ? 6 : 0 }}>
              <div style={{ width: 12, height: 12, borderRadius: 3, background: layer.color, flexShrink: 0 }} />
              <span style={{ fontSize: 11, fontWeight: 500, color: C.txt }}>{layer.name}</span>
              <span style={{ fontSize: 9, color: C.dim, fontFamily: M, marginLeft: "auto" }}>{layer.featureCount}</span>
            </div>

            {/* Symboles proportionnels — cercles superposés */}
            {cr?.type === "proportional" && (
              <div style={{ paddingLeft: 4 }}>
                <NestedCircles cr={cr} color={layer.color} />
              </div>
            )}

            {/* Traits proportionnels — lignes empilées */}
            {cr?.type === "proportional_line" && (
              <div style={{ paddingLeft: 4 }}>
                <NestedLines cr={cr} color={layer.color} />
              </div>
            )}

            {/* Catégorisée */}
            {cr?.type === "categorized" && cr.entries?.map(e => (
              <div key={e.value} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, padding: "1px 0 1px 18px" }}>
                <div style={{ width: 10, height: 10, borderRadius: 2, background: e.color, flexShrink: 0 }} />
                <span style={{ color: C.mut, flex: 1 }}>{e.value}</span>
                <span style={{ color: C.dim, fontFamily: M }}>{e.count}</span>
              </div>
            ))}

            {/* Graduée */}
            {cr?.type === "graduated" && cr.classes?.map((c, i) => (
              <div key={i} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, padding: "1px 0 1px 18px" }}>
                <div style={{ width: 10, height: 10, borderRadius: 2, background: c.color, flexShrink: 0 }} />
                <span style={{ color: C.mut, flex: 1 }}>{c.min.toFixed(1)} – {c.max.toFixed(1)}</span>
                <span style={{ color: C.dim, fontFamily: M }}>{c.count}</span>
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}
