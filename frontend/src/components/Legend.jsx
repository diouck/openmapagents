import { useThemeContext } from "../theme";
import { M } from "../config";
import { MAKI_PATHS } from "../utils/makiIcons";

// ── Preview icône Maki inline ─────────────────────────────────
function MakiPreview({ name, color = "#1D9E75", size = 18 }) {
  const paths = MAKI_PATHS[name];
  if (!paths) return null;
  return (
    <svg width={size} height={size} viewBox="0 0 15 15" xmlns="http://www.w3.org/2000/svg" style={{ flexShrink: 0 }}>
      {paths.map((d, i) => <path key={i} d={d} fill={color} />)}
    </svg>
  );
}

// ── Cercles superposés SVG — style SIG ────────────────────────
function NestedCircles({ cr, color }) {
  const maxR   = Math.min(28, Math.max(6, cr.maxSize));
  const scale  = maxR / cr.maxSize;
  const medVal = Math.round((cr.minVal + cr.maxVal) / 2);
  const medR   = (cr.minSize + cr.maxSize) / 2;
  const W      = maxR * 2 + 60;
  const H      = maxR * 2 + 4;
  const cx     = maxR + 1;
  const base   = H - 1;

  const entries = [
    { r: cr.maxSize, val: cr.maxVal },
    { r: medR,       val: medVal   },
    { r: cr.minSize, val: cr.minVal},
  ];

  return (
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} style={{ display: "block", overflow: "visible" }}>
      {entries.map(({ r, val }, i) => {
        const dr = Math.max(1.5, r * scale);
        const cy = base - dr;
        return (
          <g key={i}>
            <circle cx={cx} cy={cy} r={dr} fill={color} opacity="0.85" />
            <circle cx={cx} cy={cy} r={dr} fill="none" stroke="rgba(255,255,255,0.5)" strokeWidth="1" />
            <line
              x1={cx + dr} y1={cy}
              x2={maxR * 2 + 6} y2={cy}
              stroke="rgba(255,255,255,0.45)" strokeWidth="0.8"
              strokeDasharray="2,2"
            />
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

// ── Classes WorldCover ESA ─────────────────────────────────────
const WORLDCOVER_CLASSES = [
  { value: 10,  label: "Arbre",          color: "#006400" },
  { value: 20,  label: "Arbuste",        color: "#ffbb22" },
  { value: 30,  label: "Prairie",        color: "#ffff4c" },
  { value: 40,  label: "Culture",        color: "#f096ff" },
  { value: 50,  label: "Bâti",           color: "#fa0000" },
  { value: 60,  label: "Sol nu",         color: "#b4b4b4" },
  { value: 70,  label: "Neige / Glace",  color: "#f0f0f0" },
  { value: 80,  label: "Eau",            color: "#0064c8" },
  { value: 90,  label: "Zone humide",    color: "#0096a0" },
  { value: 95,  label: "Mangrove",       color: "#00cf75" },
  { value: 100, label: "Mousse / Lichen",color: "#fae6a0" },
];

// ── Légende raster GEE ─────────────────────────────────────────
function GeeRasterLegend({ layer }) {
  const C  = useThemeContext();
  const vp = layer.visParams;
  if (!vp) return null;

  const name = layer.name || "";
  const isWorldCover = name.includes("WorldCover") || name.includes("Occupation du sol");
  const isRGB        = name.includes("RGB") || name.includes("False Color");

  // ── WorldCover : classes catégorielles ──────────────────────
  if (isWorldCover) {
    return (
      <div style={{ paddingLeft: 4, display: "flex", flexDirection: "column", gap: 2 }}>
        {WORLDCOVER_CLASSES.map(cls => (
          <div key={cls.value} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10 }}>
            <div style={{ width: 10, height: 10, borderRadius: 2, background: cls.color, flexShrink: 0 }} />
            <span style={{ color: C.mut }}>{cls.label}</span>
          </div>
        ))}
      </div>
    );
  }

  // ── RGB : pas de gradient ───────────────────────────────────
  if (isRGB) {
    return (
      <div style={{ paddingLeft: 4, fontSize: 9, color: C.dim, fontStyle: "italic" }}>
        Composition colorée RGB
      </div>
    );
  }

  // ── Palette continue ─────────────────────────────────────────
  const palette = vp.palette;
  if (!palette?.length) return null;

  const colors   = palette.map(c => c.startsWith("#") ? c : `#${c}`);
  const gradient = `linear-gradient(to right, ${colors.join(", ")})`;
  const min = vp.min ?? 0;
  const max = vp.max ?? 1;
  const mid = (min + max) / 2;
  const fmt = v => {
    if (Math.abs(v) >= 1000) return v.toFixed(0);
    if (Math.abs(v) >= 1)    return v.toFixed(1);
    return v.toFixed(2);
  };

  return (
    <div style={{ paddingLeft: 4 }}>
      <div style={{
        height: 8, borderRadius: 4,
        background: gradient,
        margin: "3px 0 2px 0",
      }} />
      <div style={{
        display: "flex", justifyContent: "space-between",
        fontSize: 9, color: C.dim, fontFamily: M,
      }}>
        <span>{fmt(min)}</span>
        <span>{fmt(mid)}</span>
        <span>{fmt(max)}</span>
      </div>
    </div>
  );
}

// ── Légende principale ─────────────────────────────────────────
export default function Legend({ layers }) {
  const C = useThemeContext();
  const visible = layers.filter(l => l.visible);
  if (!visible.length) return null;

  return (
    <div style={{
      position: "absolute", bottom: 30, left: 10, zIndex: 10, maxWidth: 220,
      borderRadius: 8, padding: 10, maxHeight: "40vh", overflowY: "auto",
    }}>
      {visible.map(layer => {
        const cr = layer.classResult;
        const showGeeLegend = layer.isRaster && layer.visParams;

        return (
          <div key={layer.id} style={{ marginBottom: 8 }}>

            {/* Nom couche + pastille */}
            <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: (cr || showGeeLegend) ? 4 : 0 }}>
              <div style={{ width: 12, height: 12, borderRadius: 3, background: layer.color, flexShrink: 0 }} />
              <span style={{ fontSize: 11, fontWeight: 500, color: C.txt }}>{layer.name}</span>
              <span style={{ fontSize: 9, color: C.dim, fontFamily: M, marginLeft: "auto" }}>{layer.featureCount}</span>
            </div>

            {/* ── Légende raster GEE ── */}
            {showGeeLegend && <GeeRasterLegend layer={layer} />}

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

            {/* Symbole Maki / Image */}
            {cr?.type === "symbol" && (
              <div style={{ paddingLeft: 8, display: "flex", alignItems: "center", gap: 8, fontSize: 10 }}>
                {cr.symbolMode === "image" && cr.customImage?.dataUrl
                  ? <img src={cr.customImage.dataUrl} style={{ width: 20, height: 20, objectFit: "contain" }} alt="icon" />
                  : <MakiPreview name={cr.makiName || "marker"} color={cr.makiColor || "#1D9E75"} size={20} />
                }
                <div>
                  <div style={{ color: C.txt, fontWeight: 500 }}>{cr.makiName || "marker"}</div>
                  <div style={{ color: C.dim, fontSize: 9 }}>Icône Maki</div>
                </div>
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
