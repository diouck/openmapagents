import { useState } from "react";
import { useThemeContext } from "../theme";
import { F, M, EXPORT_FORMATS } from "../config";
import { Badge, Btn } from "./ui";
import ClassPanel from "./ClassPanel";

// ── Palettes prédéfinies pour rasters GEE ────────────────────
const PALETTES = {
  // Relief / Élévation
  "terrain":    { label: "Terrain",      colors: ["#313695","#74add1","#e0f3f8","#fee090","#f46d43","#a50026"] },
  "viridis":    { label: "Viridis",      colors: ["#440154","#31688e","#35b779","#fde725"] },
  "plasma":     { label: "Plasma",       colors: ["#0d0887","#7e03a8","#cc4778","#f89441","#f0f921"] },
  // Végétation
  "ndvi":       { label: "NDVI",         colors: ["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"] },
  "vert":       { label: "Vert",         colors: ["#ffffe5","#d9f0a3","#78c679","#238443","#004529"] },
  // Température
  "temperature":{ label: "Température",  colors: ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"] },
  "chaleur":    { label: "Chaleur",      colors: ["#ffffb2","#fecc5c","#fd8d3c","#f03b20","#bd0026"] },
  // Eau / humidité
  "bleu":       { label: "Bleu",         colors: ["#f7fbff","#c6dbef","#6baed6","#2171b5","#084594"] },
  "eau":        { label: "Eau/Sec",      colors: ["#8B4513","#DEB887","#ffffff","#AED6F1","#1A5276"] },
  // SAR / Radar
  "gris":       { label: "Gris",         colors: ["#000000","#ffffff"] },
  "gris_inv":   { label: "Gris inv.",    colors: ["#ffffff","#000000"] },
  // Pente / ombrage
  "pente":      { label: "Pente",        colors: ["#ffffff","#fdae61","#d73027"] },
  "ombrage":    { label: "Ombrage",      colors: ["#000000","#888888","#ffffff"] },
};

// Groupes de palettes par usage
const PALETTE_GROUPS = {
  "Relief":      ["terrain","viridis","plasma"],
  "Végétation":  ["ndvi","vert"],
  "Température": ["temperature","chaleur"],
  "Eau / SAR":   ["eau","bleu","gris","gris_inv"],
  "Pente":       ["pente","ombrage"],
};

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ── Aperçu gradient inline ─────────────────────────────────────
function PalettePreview({ colors, selected, onClick, label }) {
  const C = useThemeContext();
  const gradient = `linear-gradient(to right, ${colors.join(", ")})`;
  return (
    <div onClick={onClick} title={label} style={{
      cursor: "pointer", borderRadius: 4, overflow: "hidden",
      border: selected ? `2px solid ${C.acc}` : `1px solid ${C.bdr}`,
      height: 14, background: gradient, flexShrink: 0,
      boxShadow: selected ? `0 0 0 1px ${C.acc}` : "none",
      transition: "border .1s",
    }} />
  );
}

// ── Panel style raster GEE ─────────────────────────────────────
function RasterStylePanel({ layer, onUpdateLayer }) {
  const C = useThemeContext();
  const vp = layer.visParams;
  if (!vp) return null;

  const isRGB        = layer.name?.includes("RGB") || layer.name?.includes("False Color");
  const isWorldCover = layer.name?.includes("WorldCover") || layer.name?.includes("Occupation du sol");
  if (isRGB || isWorldCover) return null; // pas de style modifiable

  // État local calqué sur visParams actuels
  const [palKey,    setPalKey]    = useState(() => {
    // Deviner la palette actuelle
    const cur = (vp.palette || []).map(c => c.startsWith("#") ? c : "#" + c).join(",");
    return Object.entries(PALETTES).find(([, p]) => p.colors.join(",") === cur)?.[0] || "terrain";
  });
  const [minVal,    setMinVal]    = useState(vp.min ?? 0);
  const [maxVal,    setMaxVal]    = useState(vp.max ?? 1);
  const [inverted,  setInverted]  = useState(false);
  const [loading,   setLoading]   = useState(false);
  const [status,    setStatus]    = useState(null);

  const palette = PALETTES[palKey];
  const colors  = inverted ? [...palette.colors].reverse() : palette.colors;

  const applyStyle = async () => {
    if (!layer._geeParams) {
      setStatus({ type: "error", msg: "Paramètres GEE manquants — rechargez la couche" });
      return;
    }
    setLoading(true);
    setStatus(null);
    try {
      const newVis = { ...vp, palette: colors.map(c => c.replace("#","")), min: minVal, max: maxVal };
      const body = {
        ...layer._geeParams,
        vis_params_override: newVis,
      };
      const res  = await fetch(`${API}/api/gee/tiles`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);
      onUpdateLayer(layer.id, {
        tileUrl:  data.tile_url,
        visParams: { ...newVis, palette: colors },
        name:     layer.name, // garder le nom
      });
      setStatus({ type: "ok", msg: "✓ Style appliqué" });
    } catch (e) {
      setStatus({ type: "error", msg: e.message });
    }
    setLoading(false);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 7, padding: "8px 0 4px" }}>
      <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Style raster</div>

      {/* Min / Max */}
      <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
        <span style={{ fontSize: 9, color: C.dim, flexShrink: 0 }}>Min</span>
        <input type="number" value={minVal} onChange={e => setMinVal(parseFloat(e.target.value))}
          style={{ fontFamily: M, fontSize: 10, width: 60, padding: "3px 5px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }} />
        <span style={{ fontSize: 9, color: C.dim, flexShrink: 0 }}>Max</span>
        <input type="number" value={maxVal} onChange={e => setMaxVal(parseFloat(e.target.value))}
          style={{ fontFamily: M, fontSize: 10, width: 60, padding: "3px 5px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }} />
        <button onClick={() => setInverted(v => !v)} title="Inverser palette"
          style={{ fontFamily: M, fontSize: 10, padding: "3px 7px", borderRadius: 4, cursor: "pointer",
            background: inverted ? C.acc + "22" : "transparent",
            border: `0.5px solid ${inverted ? C.acc : C.bdr}`,
            color: inverted ? C.acc : C.dim, flexShrink: 0 }}>
          ⇄
        </button>
      </div>

      {/* Aperçu palette active */}
      <div style={{ height: 10, borderRadius: 4, background: `linear-gradient(to right, ${colors.join(", ")})` }} />

      {/* Groupes de palettes */}
      {Object.entries(PALETTE_GROUPS).map(([group, keys]) => (
        <div key={group}>
          <div style={{ fontSize: 8, color: C.dim, marginBottom: 3, textTransform: "uppercase", letterSpacing: ".05em" }}>{group}</div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 3 }}>
            {keys.map(k => (
              <div key={k} style={{ display: "flex", flexDirection: "column", gap: 2, cursor: "pointer" }} onClick={() => setPalKey(k)}>
                <PalettePreview colors={PALETTES[k].colors} selected={palKey === k} label={PALETTES[k].label} />
                <span style={{ fontSize: 8, color: palKey === k ? C.acc : C.dim, textAlign: "center" }}>{PALETTES[k].label}</span>
              </div>
            ))}
          </div>
        </div>
      ))}

      {/* Statut */}
      {status && (
        <div style={{ fontSize: 9, padding: "3px 6px", borderRadius: 4,
          background: (status.type === "ok" ? C.acc : C.red) + "15",
          color: status.type === "ok" ? C.acc : C.red,
          border: `0.5px solid ${(status.type === "ok" ? C.acc : C.red)}44`,
        }}>{status.msg}</div>
      )}

      {/* Bouton appliquer */}
      <button onClick={applyStyle} disabled={loading} style={{
        fontFamily: F, fontSize: 10, fontWeight: 600, padding: "6px 0",
        borderRadius: 5, width: "100%", cursor: loading ? "default" : "pointer",
        background: loading ? C.hover : C.acc,
        color: loading ? C.dim : "#fff", border: "none", opacity: loading ? 0.6 : 1,
      }}>
        {loading ? "⏳ Calcul GEE…" : "🎨 Appliquer le style"}
      </button>
    </div>
  );
}

export default function LayerPanel({ layers, onToggle, onRemove, onStyle, onExport, onClassify, onExportFmt, onRename, onMoveUp, onMoveDown, onZoomExtent, onUpdateRasterLayer, mapRef }) {
  const C = useThemeContext();
  const [exp, setExp] = useState(null);
  const [editName, setEditName] = useState(null);

  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      width: "100%",
      height: "100%",
      minHeight: 0,
      overflow: "hidden",
    }}>

      {/* En-tête fixe : "Couches N" */}
      <div style={{
        padding: "8px 14px",
        borderBottom: `0.5px solid ${C.bdr}`,
        fontSize: 12,
        fontWeight: 600,
        color: C.txt,
        flexShrink: 0,
        display: "flex",
        alignItems: "center",
        gap: 6,
      }}>
        Couches
        {layers.length > 0 && (
          <span style={{
            background: C.acc,
            color: "#fff",
            borderRadius: 8,
            fontSize: 10,
            padding: "0 6px",
            fontWeight: 700,
            lineHeight: "16px",
          }}>
            {layers.length}
          </span>
        )}
      </div>

      {/* Liste scrollable */}
      <div style={{ flex: 1, minHeight: 0, overflowY: "auto", overflowX: "hidden" }}>

        {layers.length === 0 && (
          <div style={{ padding: "20px 14px", fontSize: 11, color: C.dim, textAlign: "center" }}>
            Aucune couche chargée
          </div>
        )}

        {layers.map(l => (
          <div key={l.id} style={{ borderBottom: `0.5px solid ${C.bdr}` }}>

            {/* Ligne principale — une seule ligne par couche */}
            <div
              style={{
                padding: "7px 10px",
                display: "flex",
                alignItems: "center",
                gap: 6,
                cursor: "pointer",
                background: exp === l.id ? C.hover : "transparent",
              }}
              onClick={() => setExp(exp === l.id ? null : l.id)}
            >
              {/* Pastille couleur */}
              <div style={{
                width: 10, height: 10, borderRadius: 3,
                background: l.color, opacity: l.visible ? 1 : 0.3, flexShrink: 0,
              }} />

              {/* Nom éditable */}
              {editName === l.id ? (
                <input
                  autoFocus value={l.name}
                  onChange={e => onRename(l.id, e.target.value)}
                  onBlur={() => setEditName(null)}
                  onKeyDown={e => e.key === "Enter" && setEditName(null)}
                  onClick={e => e.stopPropagation()}
                  style={{
                    fontFamily: F, fontSize: 11, padding: "2px 6px",
                    borderRadius: 4, background: C.input, color: C.txt,
                    border: `0.5px solid ${C.acc}`, outline: "none",
                    flex: 1, minWidth: 0,
                  }}
                />
              ) : (
                <span
                  onDoubleClick={e => { e.stopPropagation(); setEditName(l.id); }}
                  title="Double-clic pour renommer"
                  style={{
                    fontSize: 11, color: l.visible ? C.txt : C.dim,
                    flex: 1, minWidth: 0,
                    overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                  }}
                >
                  {l.name}
                </span>
              )}

              {/* Actions compactes */}
              <button onClick={e => { e.stopPropagation(); onZoomExtent?.(l.id); }} title="Zoomer"
                style={{ background:"none",border:"none",cursor:"pointer",fontSize:11,padding:"0 2px",color:C.dim,lineHeight:1,flexShrink:0 }}>🔍</button>
              <button onClick={e => { e.stopPropagation(); onMoveUp?.(l.id); }} title="Monter"
                style={{ background:"none",border:"none",cursor:"pointer",fontSize:10,padding:"0 1px",color:C.dim,lineHeight:1,flexShrink:0 }}>▲</button>
              <button onClick={e => { e.stopPropagation(); onMoveDown?.(l.id); }} title="Descendre"
                style={{ background:"none",border:"none",cursor:"pointer",fontSize:10,padding:"0 1px",color:C.dim,lineHeight:1,flexShrink:0 }}>▼</button>
              {l.classResult && <span style={{ fontSize:9, color:C.acc, flexShrink:0 }} title="Classifié">●</span>}
              <button
                onClick={e => { e.stopPropagation(); onToggle(l.id); }}
                style={{
                  fontFamily: F, fontSize: 9, padding: "2px 6px", borderRadius: 4, flexShrink: 0,
                  border: `0.5px solid ${C.bdr}`,
                  background: l.visible ? "transparent" : C.acc+"22",
                  color: l.visible ? C.mut : C.acc, cursor: "pointer",
                }}
              >
                {l.visible ? "masquer" : "afficher"}
              </button>
            </div>

            {/* Panneau déroulé au clic */}
            {exp === l.id && (
              <div style={{ padding: "8px 12px 12px", display: "flex", flexDirection: "column", gap: 8, background: C.hover }}>

                {/* ── Opacité — commune raster + vecteur ── */}
                <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 11 }}>
                  <span style={{ color: C.dim }}>Opacité</span>
                  <input type="range" min="0" max="1" step="0.05" value={l.opacity}
                    onChange={e => onStyle(l.id, { opacity: parseFloat(e.target.value) })} style={{ flex: 1, height: 3 }} />
                  <span style={{ color: C.dim, fontFamily: M, flexShrink: 0 }}>{Math.round(l.opacity * 100)}%</span>
                </div>

                {/* ── Style raster GEE ── */}
                {l.isRaster && l.visParams && (
                  <RasterStylePanel layer={l} onUpdateLayer={(id, updates) => onUpdateRasterLayer?.(id, updates)} />
                )}

                {/* ── Supprimer raster ── */}
                {l.isRaster && (
                  <button onClick={() => onRemove(l.id)} style={{
                    fontFamily: F, fontSize: 10, padding: "5px 0", borderRadius: 5, width: "100%",
                    background: "transparent", border: `0.5px solid ${C.red}55`,
                    color: C.red, cursor: "pointer",
                  }}>Supprimer la couche</button>
                )}

                {/* ── Contrôles vecteur uniquement ── */}
                {!l.isRaster && (<>
                <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 11 }}>
                  <span style={{ color: C.dim }}>Couleur</span>
                  <input type="color" value={l.color} onChange={e => onStyle(l.id, { color: e.target.value })}
                    style={{ width: 24, height: 18, border: "none", borderRadius: 3, cursor: "pointer", background: "none" }} />
                  <span style={{ color: C.dim }}>Taille</span>
                  <input type="range" min="2" max="15" step="1" value={l.radius || 5}
                    onChange={e => onStyle(l.id, { radius: parseInt(e.target.value) })} style={{ flex: 1, height: 3 }} />
                  <span style={{ color: C.dim, fontFamily: M }}>{l.radius || 5}px</span>
                </div>

                <ClassPanel layer={l} classification={l.classCfg} onChange={cfg => onClassify(l.id, cfg)} mapRef={mapRef} />

                <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                  <Btn small color={C.amb} active={l.heatmap} onClick={() => onStyle(l.id, { heatmap: !l.heatmap, extrude: false })}>Heatmap</Btn>
                  <Btn small color={C.blu} active={l.extrude} onClick={() => onStyle(l.id, { extrude: !l.extrude, heatmap: false })}>3D</Btn>
                  <Btn small color={C.pnk} active={l.cluster} onClick={() => onStyle(l.id, { cluster: !l.cluster })}>Cluster</Btn>
                  <Btn small color={C.mut} active={l.labels} onClick={() => onStyle(l.id, { labels: !l.labels })}>Labels</Btn>
                </div>

                {l.extrude && (() => {
                  const numAttrs = (l.geojson?.features || []).slice(0, 10).reduce((acc, f) => {
                    Object.entries(f.properties || {}).forEach(([k, v]) => {
                      if (typeof v === "number" && v > 0 && !["id"].includes(k)) acc.add(k);
                    });
                    return acc;
                  }, new Set());
                  return numAttrs.size > 0 ? (
                    <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11 }}>
                      <span style={{ color: C.dim }}>Hauteur</span>
                      <select value={l.extrudeAttr || ""} onChange={e => onStyle(l.id, { extrudeAttr: e.target.value })}
                        style={{ fontFamily: F, fontSize: 10, padding: "3px 6px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", flex: 1 }}>
                        <option value="">auto (height)</option>
                        {[...numAttrs].map(a => <option key={a} value={a}>{a}</option>)}
                      </select>
                      <input type="range" min="1" max="20" step="1" value={l.extrudeScale || 1}
                        onChange={e => onStyle(l.id, { extrudeScale: parseInt(e.target.value) })} style={{ width: 50, height: 3 }} />
                      <span style={{ color: C.dim, fontFamily: M }}>{l.extrudeScale || 1}x</span>
                    </div>
                  ) : null;
                })()}

                {l.labels && (() => {
                  const txtAttrs = (l.geojson?.features || []).slice(0, 10).reduce((acc, f) => {
                    Object.entries(f.properties || {}).forEach(([k, v]) => {
                      if (v != null && v !== "" && !["id","geom_json"].includes(k)) acc.add(k);
                    });
                    return acc;
                  }, new Set());
                  return (
                    <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11 }}>
                      <span style={{ color: C.dim }}>Étiquette</span>
                      <select value={l.labelAttr || "name"} onChange={e => onStyle(l.id, { labelAttr: e.target.value })}
                        style={{ fontFamily: F, fontSize: 10, padding: "3px 6px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", flex: 1 }}>
                        {[...txtAttrs].map(a => <option key={a} value={a}>{a}</option>)}
                      </select>
                    </div>
                  );
                })()}

                <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                  <Btn small color={C.acc} onClick={() => onExport(l.id)}>GeoJSON</Btn>
                  {EXPORT_FORMATS.filter(f => f !== "GeoJSON").map(fmt => (
                    <Btn key={fmt} small onClick={() => onExportFmt(l.id, fmt)}>{fmt}</Btn>
                  ))}
                  <Btn small color={C.red} onClick={() => onRemove(l.id)}>Suppr.</Btn>
                </div>
                </>)}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
