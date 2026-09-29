import { useState, useRef, useEffect } from "react";
import { createPortal } from "react-dom";
import { useThemeContext } from "../theme";
import { M, F, RAMPS } from "../config";
import { MAKI_PATHS } from "../utils/makiIcons";
import { resolveChartColors } from "../utils/chartSprites";
import { IcPalette, IcMove, IcEye, IcEyeOff, IcTrash, IcZoomIn } from "../icons";


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
export default function Legend({ layers, onOpenSymbology, onReorder, onToggle, onRename, onRemove, onQuickAnalysis, onOpenSpatial, onZoomExtent, onOpenTable, onOpenFilter, onSelectEntities, onOpenDashboard, onInterpolate }) {
  const C = useThemeContext();
  const [dragId, setDragId] = useState(null);
  const [overId, setOverId] = useState(null);
  const [editId, setEditId] = useState(null);   // couche en cours de renommage
  const [menu, setMenu] = useState(null);        // menu « ⋯ » : { layer, x, y }
  // Position + taille (déplaçable + redimensionnable comme un module)
  const [pos, setPos] = useState(null);          // { x, y } ou null = ancré bas-gauche
  const [size, setSize] = useState({ w: 260, h: null }); // h null = auto (maxHeight)
  const move = useRef(null);                       // état de drag/resize en cours

  useEffect(() => {
    const MINW = 200, MINH = 110;
    const onMove = (e) => {
      const m = move.current; if (!m) return;
      const dx = e.clientX - m.sx, dy = e.clientY - m.sy;
      if (m.mode === "drag") { setPos({ x: m.px + dx, y: m.py + dy }); return; }
      // resize selon la direction (n/s/e/w + coins)
      let { x, y } = { x: m.px, y: m.py };
      let w = m.pw, h = m.ph;
      if (m.dir.includes("e")) w = Math.max(MINW, m.pw + dx);
      if (m.dir.includes("s")) h = Math.max(MINH, m.ph + dy);
      if (m.dir.includes("w")) { w = Math.max(MINW, m.pw - dx); x = m.px + (m.pw - w); }
      if (m.dir.includes("n")) { h = Math.max(MINH, m.ph - dy); y = m.py + (m.ph - h); }
      setSize({ w, h });
      setPos({ x, y });
    };
    const onUp = () => { move.current = null; };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    return () => { window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
  }, []);

  // Fixe pos/size absolus au 1er geste (la légende est sinon ancrée bas-gauche)
  const anchor = (el) => {
    const r = el.closest("[data-legend]").getBoundingClientRect();
    const p = pos || { x: r.left, y: r.top };
    const s = { w: size.w, h: size.h || r.height };
    if (!pos) setPos(p);
    if (!size.h) setSize(s);
    return { p, s };
  };
  const startDrag = (e) => {
    const { p } = anchor(e.currentTarget);
    move.current = { mode: "drag", sx: e.clientX, sy: e.clientY, px: p.x, py: p.y };
    e.preventDefault();
  };
  const startResize = (e, dir) => {
    const { p, s } = anchor(e.currentTarget);
    move.current = { mode: "resize", dir, sx: e.clientX, sy: e.clientY, px: p.x, py: p.y, pw: s.w, ph: s.h };
    e.preventDefault(); e.stopPropagation();
  };
  const runQuick = (a) => { if (menu) onQuickAnalysis?.(menu.layer, a.op, a.params); setMenu(null); };

  if (!layers.length) return null;   // afficher TOUTES les couches (visibles ou non)

  // 8 poignées de redimensionnement (bords + coins)
  const HANDLES = [
    ["n", { top: -3, left: 8, right: 8, height: 6, cursor: "ns-resize" }],
    ["s", { bottom: -3, left: 8, right: 8, height: 6, cursor: "ns-resize" }],
    ["w", { left: -3, top: 8, bottom: 8, width: 6, cursor: "ew-resize" }],
    ["e", { right: -3, top: 8, bottom: 8, width: 6, cursor: "ew-resize" }],
    ["nw", { top: -3, left: -3, width: 10, height: 10, cursor: "nwse-resize" }],
    ["ne", { top: -3, right: -3, width: 10, height: 10, cursor: "nesw-resize" }],
    ["sw", { bottom: -3, left: -3, width: 10, height: 10, cursor: "nesw-resize" }],
    ["se", { bottom: -3, right: -3, width: 10, height: 10, cursor: "nwse-resize" }],
  ];

  return (
    <div data-legend style={{
      position: "absolute", zIndex: 40,
      ...(pos ? { top: pos.y, left: pos.x } : { bottom: 30, left: 10 }),
      width: size.w, maxWidth: "calc(100vw - 20px)",
      borderRadius: 12,
      ...(size.h ? { height: size.h } : { maxHeight: "48vh" }),
      background: C.card,
      border: `0.5px solid ${C.bdr}`,
      boxShadow: "0 16px 48px rgba(0,0,0,.4)",
      backdropFilter: "blur(8px)",
    }}>
      {/* Poignées de redimensionnement (8 côtés/coins) */}
      {HANDLES.map(([dir, st]) => (
        <div key={dir} onMouseDown={e => startResize(e, dir)}
          style={{ position: "absolute", zIndex: 2, ...st }} />
      ))}

      {/* Contenu scrollable */}
      <div style={{ height: "100%", overflowY: "auto", padding: "8px 10px 9px", boxSizing: "border-box", borderRadius: 12 }}>
      {/* En-tête — poignée de déplacement */}
      <div onMouseDown={startDrag}
        style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8, paddingBottom: 6, borderBottom: `0.5px solid ${C.bdr}`, cursor: "move", userSelect: "none" }}>
        <span style={{ display: "flex", color: C.acc }}><IcPalette size={12} /></span>
        <span style={{ fontSize: 9.5, textTransform: "uppercase", letterSpacing: ".06em", color: C.dim, fontWeight: 600, flex: 1 }}>Légende</span>
        <IcMove size={11} style={{ color: C.dim }} />
      </div>
      {layers.map(layer => {
        // Détails de légende affichés uniquement pour les couches ACTIVES (visibles) ;
        // une couche masquée reste listée (grisée) pour pouvoir la réactiver.
        const cr = layer.visible ? layer.classResult : null;
        const isBivariate  = layer.visible && layer.isRaster && layer.bivariate?.palette?.length >= 9;
        const hasClasses   = layer.visible && layer.isRaster && layer.legend?.length > 0;
        const showGeeLegend = layer.visible && !isBivariate && !hasClasses && layer.isRaster
                              && (layer.visParams || _inferGeeDefaults(layer.name));

        return (
          <div key={layer.id}
            onDoubleClick={() => setEditId(layer.id)}
            title="Double-clic : renommer"
            onDragOver={e => { if (dragId && dragId !== layer.id) { e.preventDefault(); if (overId !== layer.id) setOverId(layer.id); } }}
            onDragLeave={() => setOverId(o => (o === layer.id ? null : o))}
            onDrop={e => { e.preventDefault(); if (dragId && dragId !== layer.id) onReorder?.(dragId, layer.id); setDragId(null); setOverId(null); }}
            style={{ marginBottom: 8, borderRadius: 5, cursor: "default",
              opacity: dragId === layer.id ? 0.45 : (layer.visible ? 1 : 0.45),
              boxShadow: overId === layer.id ? `inset 0 2px 0 ${C.acc}` : "none" }}>

            {/* Nom couche + poignée + bouton palette */}
            <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: (cr || showGeeLegend || isBivariate || hasClasses) ? 4 : 0 }}>
              {/* Poignée de réordonnancement */}
              <span draggable
                onDragStart={e => { setDragId(layer.id); e.dataTransfer.effectAllowed = "move"; try { e.dataTransfer.setData("text/plain", layer.id); } catch (_) {} }}
                onDragEnd={() => { setDragId(null); setOverId(null); }}
                title="Glisser pour réordonner"
                style={{ cursor: "grab", color: C.dim, lineHeight: 0, flexShrink: 0, display: "inline-flex", alignItems: "center" }}>
                <IcMove size={11} />
              </span>
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
              {editId === layer.id ? (
                <input autoFocus value={layer.name}
                  onChange={e => onRename?.(layer.id, e.target.value)}
                  onBlur={() => setEditId(null)}
                  onKeyDown={e => { if (e.key === "Enter" || e.key === "Escape") setEditId(null); }}
                  onClick={e => e.stopPropagation()} onDoubleClick={e => e.stopPropagation()}
                  style={{ fontSize: 11, flex: 1, minWidth: 0, padding: "1px 5px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.acc}`, outline: "none" }} />
              ) : (
                <span style={{ fontSize: 11, fontWeight: 500, color: C.txt, flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{layer.name}</span>
              )}
              <span style={{ fontSize: 9, color: C.dim, fontFamily: M, flexShrink: 0 }}>{layer.featureCount}</span>
              {/* Bouton activer/désactiver (bascule la visibilité, NE supprime pas) */}
              <button onClick={() => onToggle?.(layer.id)} title={layer.visible ? "Désactiver (masquer)" : "Activer (afficher)"}
                style={{ background: layer.visible ? "none" : C.acc + "18", border: `0.5px solid ${layer.visible ? C.bdr : C.acc + "66"}`, borderRadius: 5, cursor: "pointer", padding: "2px 4px", color: layer.visible ? C.dim : C.acc, lineHeight: 0, flexShrink: 0, display: "flex", alignItems: "center" }}>
                {layer.visible ? <IcEye size={12} /> : <IcEyeOff size={12} />}
              </button>
              {/* Zoom sur l'emprise de la couche */}
              <button onClick={() => onZoomExtent?.(layer.id)} title="Zoomer sur la couche"
                style={{ background: "none", border: `0.5px solid ${C.bdr}`, borderRadius: 5, cursor: "pointer", padding: "2px 4px", color: C.dim, lineHeight: 0, flexShrink: 0, display: "flex", alignItems: "center" }}>
                <IcZoomIn size={12} />
              </button>
              {/* Bouton symbologie (ouvre la fenêtre de la couche) */}
              <button onClick={() => onOpenSymbology?.(layer.id)} title="Symbologie de la couche"
                style={{ background: "none", border: `0.5px solid ${C.bdr}`, borderRadius: 5, cursor: "pointer", padding: "2px 4px", color: C.dim, lineHeight: 0, flexShrink: 0, display: "flex", alignItems: "center" }}>
                <IcPalette size={12} />
              </button>
              {/* Menu « ⋯ » — analyse rapide + ouvrir dans Analyse spatiale */}
              <button onClick={e => { const r = e.currentTarget.getBoundingClientRect(); setMenu({ layer, x: r.left, y: r.bottom + 2, yUp: r.top - 2 }); }}
                title="Plus d'actions (analyse, table…)"
                style={{ background: menu?.layer?.id === layer.id ? C.acc + "22" : "none", border: `0.5px solid ${menu?.layer?.id === layer.id ? C.acc : C.bdr}`, borderRadius: 5, cursor: "pointer", padding: "1px 5px", color: menu?.layer?.id === layer.id ? C.acc : C.dim, lineHeight: 1, flexShrink: 0, display: "flex", alignItems: "center", fontWeight: 700, fontSize: 13 }}>
                ⋯
              </button>
              {/* Corbeille — supprimer la couche */}
              <button onClick={() => onRemove?.(layer.id)} title="Supprimer la couche"
                style={{ background: "none", border: `0.5px solid ${C.bdr}`, borderRadius: 5, cursor: "pointer", padding: "2px 4px", color: C.dim, lineHeight: 0, flexShrink: 0, display: "flex", alignItems: "center" }}
                onMouseEnter={e => { e.currentTarget.style.color = C.red; e.currentTarget.style.borderColor = C.red; }}
                onMouseLeave={e => { e.currentTarget.style.color = C.dim; e.currentTarget.style.borderColor = C.bdr; }}>
                <IcTrash size={12} />
              </button>
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
            {layer.visible && layer.chartCfg?.vars?.length > 0 && (() => {
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

            {/* 2e style : taille proportionnelle (combinée à la couleur) */}
            {layer.visible && layer.sizeResult?.radiusExpression && cr?.type !== "proportional" && (
              <div style={{ paddingLeft: 4, color: C.mut, marginTop: 2 }}>
                <NestedCircles cr={{ minSize: layer.sizeResult.minSize, maxSize: layer.sizeResult.maxSize, minVal: layer.sizeResult.minVal, maxVal: layer.sizeResult.maxVal, classes: layer.sizeResult.color ? null : layer.classResult?.classes }} color={layer.sizeResult.color || layer.color} />
              </div>
            )}
            {layer.visible && layer.sizeResult?.widthExpression && cr?.type !== "proportional_line" && (
              <div style={{ paddingLeft: 4, color: C.mut, marginTop: 2 }}>
                <NestedLines cr={{ minSize: layer.sizeResult.minSize, maxSize: layer.sizeResult.maxSize, minVal: layer.sizeResult.minVal, maxVal: layer.sizeResult.maxVal }} color={layer.color} />
              </div>
            )}

            {/* Style bivarié (vecteur) — matrice 3×3 */}
            {layer.visible && layer.biv?.palette && <BivariateLegend bivariate={layer.biv} />}
          </div>
        );
      })}
      </div>{/* fin contenu scrollable */}

      {/* Menu « ⋯ » (popup) */}
      {menu && (() => {
        const l = menu.layer;
        const isVec = !l.isRaster && l.geojson;
        const item = (label, onClick, accent) => (
          <button onClick={onClick}
            style={{ width: "100%", textAlign: "left", padding: "8px 11px", background: "transparent", border: "none", color: accent ? C.acc : C.txt, cursor: "pointer", fontSize: 12, fontFamily: F, fontWeight: accent ? 600 : 400 }}
            onMouseEnter={e => e.currentTarget.style.background = C.hover}
            onMouseLeave={e => e.currentTarget.style.background = "transparent"}>{label}</button>
        );
        const sep = (k) => <div key={k} style={{ height: 1, background: C.bdr, margin: "3px 0" }} />;
        const hdr = (t) => <div style={{ padding: "6px 11px 2px", fontSize: 9, letterSpacing: ".05em", textTransform: "uppercase", color: C.dim }}>{t}</div>;

        // Géométrie de la couche → outils adaptés (points / lignes / polygones).
        const gt = (l.geojson?.features?.find(f => f.geometry)?.geometry?.type) || "";
        const geom = /Point/.test(gt) ? "point" : /LineString/.test(gt) ? "line" : /Polygon/.test(gt) ? "polygon" : "";
        const QUICK = {
          point:   [{ label: "Tampon 500 m", op: "buffer", params: { radius: 500 } }, { label: "Tampon 1 km", op: "buffer", params: { radius: 1000 } }, { label: "Enveloppe convexe", op: "convex_hull", params: {} }],
          line:    [{ label: "Tampon 500 m", op: "buffer", params: { radius: 500 } }, { label: "Enveloppe convexe", op: "convex_hull", params: {} }],
          polygon: [{ label: "Centroïdes", op: "centroid", params: {} }, { label: "Tampon 1 km", op: "buffer", params: { radius: 1000 } }, { label: "Enveloppe convexe", op: "convex_hull", params: {} }],
        }[geom] || [];
        const geomLbl = { point: "Outils points", line: "Outils lignes", polygon: "Outils polygones" }[geom] || "Outils";

        // Positionnement responsive : s'ouvre vers le bas si la place suffit, sinon
        // vers le haut ; hauteur bornée + scroll pour ne jamais déborder l'écran.
        const vw = window.innerWidth, vh = window.innerHeight;
        const openUp = (vh - menu.y) < 300 && menu.yUp > vh / 2;
        const left = Math.max(6, Math.min(menu.x, vw - 246));
        const posStyle = openUp
          ? { bottom: vh - menu.yUp, maxHeight: menu.yUp - 12 }
          : { top: menu.y, maxHeight: vh - menu.y - 12 };

        return createPortal(
          <>
            <div onClick={() => setMenu(null)} style={{ position: "fixed", inset: 0, zIndex: 10060 }} />
            <div style={{ position: "fixed", left, zIndex: 10061, minWidth: 220, maxWidth: 260, background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 8, boxShadow: "0 8px 24px rgba(0,0,0,.35)", overflowY: "auto", ...posStyle }}>
              <div style={{ padding: "7px 11px", fontSize: 9.5, letterSpacing: ".05em", textTransform: "uppercase", color: C.dim, borderBottom: `0.5px solid ${C.bdr}`, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", position: "sticky", top: 0, background: C.card }}>{l.name}</div>
              {isVec && onOpenTable && item("Ouvrir la table attributaire", () => { onOpenTable(l); setMenu(null); })}
              {isVec && onOpenFilter && item("Filtrer par attribut", () => { onOpenFilter(l); setMenu(null); })}
              {isVec && onOpenDashboard && item("Tableau de bord", () => { onOpenDashboard(l); setMenu(null); })}
              {isVec && onInterpolate && geom === "point" && item("Interpolation (kriging/IDW)", () => { onInterpolate(l); setMenu(null); })}
              {isVec && onSelectEntities && item("Sélectionner des entités (clic)", () => { onSelectEntities(l); setMenu(null); })}
              {isVec && QUICK.length > 0 && <>
                {sep("s0")}
                {hdr(geomLbl)}
                {QUICK.map((a, i) => <div key={i}>{item(a.label, () => runQuick(a))}</div>)}
              </>}
              {onOpenSpatial && <>
                {sep("s")}
                {hdr("Ouvrir dans Analyse spatiale")}
                {isVec && item("→ Vecteur", () => { onOpenSpatial(l.id, "vecteur"); setMenu(null); }, true)}
                {l.isRaster && item("→ Raster", () => { onOpenSpatial(l.id, "raster"); setMenu(null); }, true)}
                {item("→ Avancé", () => { onOpenSpatial(l.id, "avance"); setMenu(null); }, true)}
              </>}
            </div>
          </>,
          document.body
        );
      })()}
    </div>
  );
}
