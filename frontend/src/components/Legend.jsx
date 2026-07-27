import { useThemeContext } from "../theme";
import { M, RAMPS } from "../config";
import { MAKI_PATHS } from "../utils/makiIcons";
import { resolveChartColors } from "../utils/chartSprites";

// ── Formatage surface ──────────────────────────────────────────────────────────
function fmtArea(ha) {
  if (ha === null || ha === undefined || ha === 0) return null;
  if (ha < 1)   return `${Math.round(ha * 10000)} m²`;
  if (ha < 100) return `${ha.toFixed(1)} ha`;
  return `${(ha / 100).toFixed(2)} km²`;
}

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

  // Si une palette graduée est associée (cf. buildClassification), chaque cercle
  // reprend la couleur de sa classe — sinon tous prennent la couleur de la couche.
  const cls = cr.classes || [];
  const colAt = (i, n) => cls.length ? (cls[Math.round(i / (n - 1) * (cls.length - 1))]?.color || color) : color;
  const entries = [
    { r: cr.maxSize, val: cr.maxVal, c: colAt(2, 3) },
    { r: medR,       val: medVal,    c: colAt(1, 3) },
    { r: cr.minSize, val: cr.minVal, c: colAt(0, 3) },
  ];

  return (
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} style={{ display: "block", overflow: "visible" }}>
      {entries.map(({ r, val, c }, i) => {
        const dr = Math.max(1.5, r * scale);
        const cy = base - dr;
        return (
          <g key={i}>
            <circle cx={cx} cy={cy} r={dr} fill={c || color} opacity="0.85" />
            <circle cx={cx} cy={cy} r={dr} fill="none" stroke="rgba(255,255,255,0.5)" strokeWidth="1" />
            <line
              x1={cx + dr} y1={cy}
              x2={maxR * 2 + 6} y2={cy}
              stroke="rgba(255,255,255,0.45)" strokeWidth="0.8"
              strokeDasharray="2,2"
            />
            <text
              x={maxR * 2 + 8} y={cy + 3.5}
              fontSize="9" fill="currentColor" opacity="0.7"
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
              fontSize="9" fill="currentColor" opacity="0.7"
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

// ── Palettes par défaut par index GEE ────────────────────────
const GEE_DEFAULT_PALETTES = {
  NDVI:  { palette: ["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"], min: -0.2, max: 0.9,  unit: "NDVI" },
  EVI:   { palette: ["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"], min: -0.2, max: 0.9,  unit: "EVI"  },
  SAVI:  { palette: ["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#1a9850"],           min: -0.5, max: 1.0,  unit: "SAVI" },
  NDWI:  { palette: ["#d7191c","#fdae61","#ffffbf","#abd9e9","#2c7bb6"],                              min: -0.5, max: 0.5,  unit: "NDWI" },
  MNDWI: { palette: ["#d7191c","#fdae61","#ffffbf","#abd9e9","#2c7bb6"],                              min: -0.5, max: 0.5,  unit: "MNDWI"},
  NBR:   { palette: ["#006837","#31a354","#78c679","#c2e699","#ffffcc","#feb24c","#f03b20","#bd0026"],  min: -1,   max: 1,   unit: "NBR"  },
  LST:   { palette: ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"], min: 0,    max: 45,  unit: "°C"   },
  SAR:   { palette: ["#000000","#404040","#808080","#bfbfbf","#ffffff"],                               min: -25,  max: 0,   unit: "dB"   },
};

function _inferGeeDefaults(name) {
  const n = (name || "").toUpperCase();
  for (const [key, val] of Object.entries(GEE_DEFAULT_PALETTES)) {
    if (n.includes(key)) return { ...val, key };
  }
  if (n.includes("TEMPERATURE") || n.includes("SURFACE") || n.includes("CHALEUR") || n.includes("ICU")) {
    return { ...GEE_DEFAULT_PALETTES.LST, key: "LST" };
  }
  return null;
}

// ── Légende raster GEE ─────────────────────────────────────────
function GeeRasterLegend({ layer }) {
  const C  = useThemeContext();
  const vp = layer.visParams || _inferGeeDefaults(layer.name);
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
        <span style={{ color: "var(--c-acc,#1D9E75)", fontWeight: 500 }}>{vp.unit || ""}</span>
        <span>{fmt(max)}</span>
      </div>
    </div>
  );
}

// ── Légende bivariée (matrice 3×3 sémiologie croisée) ──────────
function BivariateLegend({ bivariate }) {
  const C = useThemeContext();
  const pal = bivariate?.palette || [];
  if (pal.length < 9) return null;

  const cell   = 17;
  const labelA = bivariate.label_a || "Variable A";
  const labelB = bivariate.label_b || "Variable B";
  const lvl    = bivariate.levels || ["Faible", "Moyen", "Élevé"];

  return (
    <div style={{ paddingLeft: 4, paddingTop: 2 }}>
      <div style={{ display: "flex", gap: 5 }}>
        {/* Axe A (vertical, Faible bas → Élevé haut) */}
        <div style={{ display: "flex", alignItems: "center" }}>
          <span style={{
            fontSize: 8, color: C.dim, writingMode: "vertical-rl",
            transform: "rotate(180deg)", whiteSpace: "nowrap",
            maxHeight: cell * 3 + 3, overflow: "hidden", textOverflow: "ellipsis",
          }} title={labelA}>{labelA} →</span>
        </div>

        <div>
          {/* Grille 3×3 */}
          <div style={{ display: "grid", gridTemplateColumns: `repeat(3, ${cell}px)`, gridTemplateRows: `repeat(3, ${cell}px)`, gap: 1.5 }}>
            {[2, 1, 0].map(a =>
              [0, 1, 2].map(b => {
                const code = a * 3 + b;
                return (
                  <div key={code}
                    title={`${labelA} : ${lvl[a]} · ${labelB} : ${lvl[b]}`}
                    style={{ width: cell, height: cell, background: pal[code], borderRadius: 2, border: "0.5px solid rgba(0,0,0,.12)" }} />
                );
              })
            )}
          </div>
          {/* Axe B (horizontal, Faible gauche → Élevé droite) */}
          <div style={{ fontSize: 8, color: C.dim, marginTop: 2, maxWidth: cell * 3 + 3, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={labelB}>
            {labelB} →
          </div>
        </div>
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
      position: "absolute", bottom: 30, left: 10, zIndex: 10, maxWidth: 240,
      borderRadius: 8, padding: "8px 10px", maxHeight: "45vh", overflowY: "auto",
      background: C.card,
      border: `1px solid ${C.bdr}`,
      boxShadow: "0 4px 20px rgba(0,0,0,0.25)",
      backdropFilter: "blur(8px)",
    }}>
      {visible.map(layer => {
        const cr = layer.classResult;
        const isBivariate  = layer.isRaster && layer.bivariate?.palette?.length >= 9;
        // Couche classée : sa légende porte les vraies bornes min/max de chaque
        // classe. La rampe continue tirée de visParams afficherait « 0 → n-1 »
        // (les identifiants de classe), ce qui n'a aucun sens pour le lecteur.
        const hasClasses   = layer.isRaster && layer.legend?.length > 0;
        const showGeeLegend = !isBivariate && !hasClasses && layer.isRaster
                              && (layer.visParams || _inferGeeDefaults(layer.name));

        return (
          <div key={layer.id} style={{ marginBottom: 8 }}>

            {/* Nom couche + pastille */}
            <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: (cr || showGeeLegend || isBivariate || hasClasses) ? 4 : 0 }}>
              {/* Pastille adaptée au type de couche */}
              {layer.theme === "isochrone" ? (
                <div style={{ width: 14, height: 10, borderRadius: 3, border: `2px solid ${layer.color}`, background: layer.color + "55", flexShrink: 0 }} />
              ) : layer.theme === "route" ? (
                <div style={{ width: 14, height: 3, borderRadius: 2, background: layer.color, flexShrink: 0, marginTop: 4 }} />
              ) : (layer.geojson?.features?.[0]?.geometry?.type === "Point" || !layer.geojson?.features?.[0]) ? (
                <div style={{ width: 10, height: 10, borderRadius: "50%", background: layer.color, flexShrink: 0 }} />
              ) : (
                <div style={{ width: 12, height: 12, borderRadius: 3, background: layer.color, flexShrink: 0 }} />
              )}
              <span style={{ fontSize: 11, fontWeight: 500, color: C.txt }}>{layer.name}</span>
              <span style={{ fontSize: 9, color: C.dim, fontFamily: M, marginLeft: "auto" }}>{layer.featureCount}</span>
            </div>

            {/* ── Légende raster GEE (palettes continues / WorldCover) ── */}
            {showGeeLegend && <GeeRasterLegend layer={layer} />}

            {/* ── Légende bivariée (matrice 3×3) ── */}
            {isBivariate && <BivariateLegend bivariate={layer.bivariate} />}

            {/* ── Légende classification raster (classif supervisée / auto / cluster) ── */}
            {hasClasses && !isBivariate && (
              <div style={{ paddingLeft: 4, display: "flex", flexDirection: "column", gap: 3 }}>
                {layer.legend.map(e => (
                  <div key={e.class_id ?? e.label}
                       style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    <div style={{
                      width: 10, height: 10, borderRadius: 2, flexShrink: 0,
                      background: e.color,
                      border: "0.5px solid rgba(0,0,0,.12)",
                    }} />
                    <span style={{ fontSize: 10, color: C.mut, flex: 1,
                                   overflow: "hidden", textOverflow: "ellipsis",
                                   whiteSpace: "nowrap" }}>
                      {e.label}
                    </span>
                    {fmtArea(e.area_ha) && (
                      <span style={{ fontSize: 9, color: C.dim, fontFamily: M,
                                     flexShrink: 0, whiteSpace: "nowrap" }}>
                        {fmtArea(e.area_ha)}
                      </span>
                    )}
                  </div>
                ))}
              </div>
            )}

            {/* Graphiques par entité — couleur de chaque variable représentée */}
            {layer.chartCfg?.vars?.length > 0 && (() => {
              const cs = resolveChartColors(layer.chartCfg, RAMPS, layer.chartCfg.vars.length);
              return (
                <div style={{ paddingLeft: 4, display: "flex", flexDirection: "column", gap: 2 }}>
                  {layer.chartCfg.vars.map((v, i) => (
                    <div key={v} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10 }}>
                      <span style={{ width: 10, height: 10, borderRadius: 2, flexShrink: 0, background: cs[i % cs.length] }} />
                      <span style={{ color: C.mut, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{v}</span>
                    </div>
                  ))}
                </div>
              );
            })()}

            {/* Symboles proportionnels — cercles superposés */}
            {cr?.type === "proportional" && (
              <div style={{ paddingLeft: 4, color: C.mut }}>
                <NestedCircles cr={cr} color={layer.color} />
              </div>
            )}

            {/* Traits proportionnels — lignes empilées */}
            {cr?.type === "proportional_line" && (
              <div style={{ paddingLeft: 4, color: C.mut }}>
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
