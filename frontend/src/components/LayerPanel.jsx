import { useState, useEffect, useRef } from "react";
import { createPortal } from "react-dom";
import { useThemeContext } from "../theme";
import { F, M, EXPORT_FORMATS, BIVARIATE_PALETTES } from "../config";
import { getLayerAttrs, buildBivariate } from "../utils/classification";
import { Badge, Btn } from "./ui";
import { IcBarChart, IcEye, IcEyeOff, IcPalette, IcMove, IcMaximize, IcMinus, IcX,
  IcHash, IcSliders, IcTable, IcInfo, IcCopy, IcClipboard, IcFileDown, IcRefresh, IcTrash, IcCheck, IcEdit, IcVenn } from "../icons";
import ClassPanel from "./ClassPanel";
import ChartStyleBlock from "./ChartStyleBlock";
import FieldCalcBlock from "./FieldCalcBlock";
import IndexStatsModal from "./IndexStatsModal";
import FilterModal, { FunnelIcon, applyFilter } from "./FilterModal";
import { getPC, applyPCStyle, ASPRS_CLASSES } from "../utils/lidarStyle";
import { nextZ, bumpZ } from "../utils/zorder";

// ── Palettes prédéfinies pour rasters GEE ────────────────────
const PALETTES = {
  "terrain":    { label: "Terrain",      colors: ["#313695","#74add1","#e0f3f8","#fee090","#f46d43","#a50026"] },
  "viridis":    { label: "Viridis",      colors: ["#440154","#31688e","#35b779","#fde725"] },
  "plasma":     { label: "Plasma",       colors: ["#0d0887","#7e03a8","#cc4778","#f89441","#f0f921"] },
  "ndvi":       { label: "NDVI",         colors: ["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"] },
  "vert":       { label: "Vert",         colors: ["#ffffe5","#d9f0a3","#78c679","#238443","#004529"] },
  "temperature":{ label: "Température",  colors: ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"] },
  "chaleur":    { label: "Chaleur",      colors: ["#ffffb2","#fecc5c","#fd8d3c","#f03b20","#bd0026"] },
  "bleu":       { label: "Bleu",         colors: ["#f7fbff","#c6dbef","#6baed6","#2171b5","#084594"] },
  "eau":        { label: "Eau/Sec",      colors: ["#8B4513","#DEB887","#ffffff","#AED6F1","#1A5276"] },
  "gris":       { label: "Gris",         colors: ["#000000","#ffffff"] },
  "gris_inv":   { label: "Gris inv.",    colors: ["#ffffff","#000000"] },
  "pente":      { label: "Pente",        colors: ["#ffffff","#fdae61","#d73027"] },
  "ombrage":    { label: "Ombrage",      colors: ["#000000","#888888","#ffffff"] },
};

const PALETTE_GROUPS = {
  "Relief":      ["terrain","viridis","plasma"],
  "Végétation":  ["ndvi","vert"],
  "Température": ["temperature","chaleur"],
  "Eau / SAR":   ["eau","bleu","gris","gris_inv"],
  "Pente":       ["pente","ombrage"],
};

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ── Formatage surface (identique à ClassifMetricsModal) ───────
function fmtArea(ha) {
  if (ha === null || ha === undefined || ha === 0) return null;
  if (ha < 1)   return `${Math.round(ha * 10000)} m²`;
  if (ha < 100) return `${ha.toFixed(1)} ha`;
  return `${(ha / 100).toFixed(2)} km²`;
}

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
  if (isRGB || isWorldCover) return null;

  const [palKey,   setPalKey]   = useState(() => {
    const cur = (vp.palette || []).map(c => c.startsWith("#") ? c : "#" + c).join(",");
    return Object.entries(PALETTES).find(([, p]) => p.colors.join(",") === cur)?.[0] || "terrain";
  });
  const [minVal,   setMinVal]   = useState(vp.min ?? 0);
  const [maxVal,   setMaxVal]   = useState(vp.max ?? 1);
  const [inverted, setInverted] = useState(false);
  const [classify, setClassify] = useState("none"); // none | quantile | jenks | equal
  const [nClasses, setNClasses] = useState(5);
  const [loading,  setLoading]  = useState(false);
  const [status,   setStatus]   = useState(null);

  const palette = PALETTES[palKey];
  const colors  = inverted ? [...palette.colors].reverse() : palette.colors;

  // opts.auto → min/max automatiques (percentiles p2/p98 sur l'emprise, backend)
  const applyStyle = async (opts = {}) => {
    if (!layer._geeParams) {
      setStatus({ type: "error", msg: "Paramètres GEE manquants — rechargez la couche" });
      return;
    }
    setLoading(true);
    setStatus(null);
    try {
      const newVis = { ...vp, palette: colors.map(c => c.replace("#","")), min: minVal, max: maxVal };
      const body = { ...layer._geeParams, vis_params_override: newVis };
      if (opts.auto) body.auto_stretch = true;
      if (classify !== "none") { body.classify = classify; body.n_classes = nClasses; }
      const res  = await fetch(`${API}/api/gee/tiles`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);
      // Refléter les min/max renvoyés (auto-stretch) dans les champs.
      const dvp = data.vis_params || {};
      if (opts.auto && typeof dvp.min === "number") { setMinVal(dvp.min); setMaxVal(dvp.max); }
      const discrete = Array.isArray(data.legend) && data.legend.length > 0;
      onUpdateLayer(layer.id, {
        tileUrl:   data.tile_url,
        visParams: discrete ? { ...dvp } : { ...newVis, palette: colors, ...(opts.auto ? { min: dvp.min, max: dvp.max } : {}) },
        legend:    discrete ? data.legend : null,   // classifié → légende discrète ; sinon rampe
        name:      layer.name,
      });
      setStatus({ type: "ok", msg: opts.auto ? "✓ Min/max automatiques" : classify !== "none" ? `✓ ${nClasses} classes appliquées` : "✓ Style appliqué" });
    } catch (e) {
      setStatus({ type: "error", msg: e.message });
    }
    setLoading(false);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 7, padding: "8px 0 4px" }}>
      <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Style raster</div>

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
        <button onClick={() => applyStyle({ auto: true })} disabled={loading} title="Min/max automatiques (percentiles 2–98 % sur l'emprise)"
          style={{ fontFamily: F, fontSize: 9, padding: "3px 7px", borderRadius: 4, cursor: loading ? "default" : "pointer",
            background: "transparent", border: `0.5px solid ${C.acc}55`, color: C.acc, flexShrink: 0 }}>
          Auto
        </button>
      </div>

      {/* ── Classification (comme les couches vecteur) ── */}
      <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
        <span style={{ fontSize: 9, color: C.dim, flexShrink: 0 }}>Classes</span>
        <select value={classify} onChange={e => setClassify(e.target.value)}
          style={{ fontFamily: F, fontSize: 10, flex: 1, padding: "3px 5px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }}>
          <option value="none">Continu (rampe)</option>
          <option value="quantile">Quantiles</option>
          <option value="jenks">Jenks (naturelles)</option>
          <option value="equal">Intervalles égaux</option>
        </select>
        {classify !== "none" && (
          <input type="number" min="2" max="12" value={nClasses} title="Nombre de classes"
            onChange={e => setNClasses(Math.max(2, Math.min(12, parseInt(e.target.value) || 5)))}
            style={{ fontFamily: M, fontSize: 10, width: 42, padding: "3px 5px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }} />
        )}
      </div>
      {classify !== "none" && (
        <div style={{ fontSize: 8, color: C.dim }}>Min/max ignorés en mode classé — les seuils sont calculés sur les données.</div>
      )}

      <div style={{ height: 10, borderRadius: 4, background: `linear-gradient(to right, ${colors.join(", ")})` }} />

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

      {status && (
        <div style={{ fontSize: 9, padding: "3px 6px", borderRadius: 4,
          background: (status.type === "ok" ? C.acc : C.red) + "15",
          color: status.type === "ok" ? C.acc : C.red,
          border: `0.5px solid ${(status.type === "ok" ? C.acc : C.red)}44`,
        }}>{status.msg}</div>
      )}

      <button onClick={() => applyStyle()} disabled={loading} style={{
        fontFamily: F, fontSize: 10, fontWeight: 600, padding: "6px 0",
        borderRadius: 5, width: "100%", cursor: loading ? "default" : "pointer",
        background: loading ? C.hover : C.acc,
        color: loading ? C.dim : "#fff", border: "none", opacity: loading ? 0.6 : 1,
      }}>
        {loading ? "Calcul GEE…" : classify !== "none" ? "Appliquer la classification" : "Appliquer le style"}
      </button>
    </div>
  );
}

// ── Reclassification raster importé (GeoTIFF mono-bande) — façon QGIS ──
// Méthode (continu / égaux / quantiles / Jenks / manuel) puis TABLE DE CLASSES
// éditable (borne max, couleur, libellé, ajout/suppression).
function RasterImageStylePanel({ layer, onUpdate }) {
  const C = useThemeContext();
  const [palKey, setPalKey] = useState("terrain");
  const [band, setBand] = useState(1);
  const [minVal, setMinVal] = useState(layer.vmin ?? (layer.bandRanges?.[0]?.[0] ?? 0));
  const [maxVal, setMaxVal] = useState(layer.vmax ?? (layer.bandRanges?.[0]?.[1] ?? 1));
  const [method, setMethod] = useState("continu");   // continu|equal|quantile|jenks|manual
  const [nClasses, setNClasses] = useState(5);
  const [inverted, setInverted] = useState(false);
  const [table, setTable] = useState(null);          // { edges:[], colors:[], labels:[] }
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState(null);

  const palette = PALETTES[palKey] || PALETTES.terrain;
  const rampColors = inverted ? [...palette.colors].reverse() : palette.colors;

  const legendToTable = (legend) => ({
    edges: [legend[0].min, ...legend.map(l => l.max)],
    colors: legend.map(l => l.color),
    labels: legend.map(l => l.label),
    alphas: legend.map(l => (l.hidden ? 0 : 255)),
  });

  const restyle = async (body, patch) => {
    setLoading(true); setStatus(null);
    try {
      const fd = new FormData();
      fd.append("raster_token", layer.rasterToken);
      fd.append("palette", rampColors.map(c => c.replace("#", "")).join(","));
      fd.append("vmin", String(minVal)); fd.append("vmax", String(maxVal));
      fd.append("band", String(band));
      Object.entries(body).forEach(([k, v]) => fd.append(k, String(v)));
      const res = await fetch(`${API}/api/raster/restyle`, { method: "POST", body: fd });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);
      onUpdate(layer.id, { imageUrl: `data:image/png;base64,${data.png_b64}`, vmin: minVal, vmax: maxVal, legend: data.legend?.length ? data.legend : null, styleV: (layer.styleV || 0) + 1 });
      if (patch) patch(data);
      setStatus({ type: "ok", msg: "✓ Appliqué" });
    } catch (e) { setStatus({ type: "error", msg: e.message }); }
    setLoading(false);
  };

  const classify = () => {
    if (method === "continu") { setTable(null); return restyle({ classes: 0 }); }
    return restyle({ classes: nClasses, classify: method },
      (data) => { if (data.legend?.length) setTable(legendToTable(data.legend)); });
  };
  const applyManual = () => {
    if (!table) return;
    return restyle({ classify: "manual", breaks: table.edges.join(","), class_colors: table.colors.map(c => c.replace("#", "")).join(","), class_alpha: (table.alphas || table.colors.map(() => 255)).join(",") });
  };

  // ── édition de la table ──
  const setEdge = (i, v) => setTable(t => { const e = [...t.edges]; const val = parseFloat(v); if (!isNaN(val) && val > e[i] && val < e[i + 2]) e[i + 1] = val; return { ...t, edges: e }; });
  const setColor = (i, c) => setTable(t => { const cc = [...t.colors]; cc[i] = c; return { ...t, colors: cc }; });
  const setLabel = (i, l) => setTable(t => { const ll = [...t.labels]; ll[i] = l; return { ...t, labels: ll }; });
  const toggleVis = (i) => setTable(t => { const a = [...(t.alphas || t.colors.map(() => 255))]; a[i] = a[i] === 0 ? 255 : 0; return { ...t, alphas: a }; });
  const delRow = (i) => setTable(t => {
    if (t.colors.length <= 2) return t;
    const edges = [...t.edges], colors = [...t.colors], labels = [...t.labels], alphas = [...(t.alphas || t.colors.map(() => 255))];
    edges.splice(i < colors.length - 1 ? i + 1 : i, 1); colors.splice(i, 1); labels.splice(i, 1); alphas.splice(i, 1);
    return { edges, colors, labels, alphas };
  });
  const addRow = () => setTable(t => {
    const n = t.colors.length; const mid = (t.edges[n - 1] + t.edges[n]) / 2;
    return { edges: [...t.edges.slice(0, n), mid, t.edges[n]], colors: [...t.colors, t.colors[n - 1]], labels: [...t.labels, ""], alphas: [...(t.alphas || t.colors.map(() => 255)), 255] };
  });

  const iSt = { fontFamily: M, fontSize: 10, width: 54, padding: "3px 5px", borderRadius: 4, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" };
  const METHODS = [["continu", "Continu"], ["equal", "Égaux"], ["quantile", "Quantiles"], ["jenks", "Jenks"], ["manual", "Manuel"]];

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 7, padding: "8px 0 4px" }}>
      <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Reclassification raster{layer.bands > 1 ? ` (${layer.bands} bandes)` : ""}</div>

      {layer.bands > 1 && (
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          <span style={{ fontSize: 9, color: C.dim }}>Bande</span>
          <select value={band} onChange={e => { const b = parseInt(e.target.value); setBand(b); const r = layer.bandRanges?.[b - 1]; if (r) { setMinVal(r[0]); setMaxVal(r[1]); } }} style={{ ...iSt, width: "auto", fontFamily: F }}>
            {Array.from({ length: layer.bands }, (_, i) => <option key={i} value={i + 1}>Bande {i + 1}</option>)}
          </select>
        </div>
      )}

      <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
        <span style={{ fontSize: 9, color: C.dim }}>Min</span><input type="number" value={minVal} onChange={e => setMinVal(parseFloat(e.target.value))} style={iSt} />
        <span style={{ fontSize: 9, color: C.dim }}>Max</span><input type="number" value={maxVal} onChange={e => setMaxVal(parseFloat(e.target.value))} style={iSt} />
        <button onClick={() => setInverted(v => !v)} title="Inverser palette" style={{ fontFamily: M, fontSize: 10, padding: "3px 7px", borderRadius: 4, cursor: "pointer", background: inverted ? C.acc + "22" : "transparent", border: `0.5px solid ${inverted ? C.acc : C.bdr}`, color: inverted ? C.acc : C.dim }}>⇄</button>
      </div>

      <div style={{ display: "flex", gap: 5, alignItems: "center", flexWrap: "wrap" }}>
        <select value={method} onChange={e => setMethod(e.target.value)} style={{ ...iSt, width: "auto", fontFamily: F }}>
          {METHODS.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
        {method !== "continu" && method !== "manual" && (<><span style={{ fontSize: 9, color: C.dim }}>Classes</span><input type="number" min="2" max="12" value={nClasses} onChange={e => setNClasses(Math.max(2, Math.min(12, parseInt(e.target.value) || 2)))} style={iSt} /></>)}
        <button onClick={classify} disabled={loading} style={{ fontFamily: F, fontSize: 10, fontWeight: 600, padding: "4px 10px", borderRadius: 5, cursor: loading ? "default" : "pointer", background: C.acc, color: "#fff", border: "none", opacity: loading ? 0.6 : 1, marginLeft: "auto" }}>{loading ? "…" : (method === "continu" ? "Appliquer" : "Classer")}</button>
      </div>

      {layer.dataMin != null && <div style={{ fontSize: 8, color: C.dim }}>données {layer.dataMin}–{layer.dataMax}</div>}

      {method === "continu" ? (
        <>
          <div style={{ height: 10, borderRadius: 4, background: `linear-gradient(to right, ${rampColors.join(", ")})` }} />
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
        </>
      ) : table ? (
        <>
          <div style={{ fontSize: 8, color: C.dim }}>👁 masquer (transparent) · borne · couleur · libellé — puis « Appliquer les classes ».</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 2, maxHeight: 190, overflowY: "auto" }}>
            {table.colors.map((col, i) => { const hidden = (table.alphas || [])[i] === 0; return (
              <div key={i} style={{ display: "flex", alignItems: "center", gap: 5, fontFamily: M, fontSize: 9.5, opacity: hidden ? 0.45 : 1 }}>
                <button onClick={() => toggleVis(i)} title={hidden ? "Afficher cette classe" : "Masquer (transparent)"} style={{ background: "none", border: "none", cursor: "pointer", padding: 0, display: "flex", flexShrink: 0, color: hidden ? C.dim : C.acc }}>{hidden ? <IcEyeOff size={13} /> : <IcEye size={13} />}</button>
                <input type="color" value={col} onChange={e => setColor(i, e.target.value)} style={{ width: 20, height: 18, padding: 0, border: `0.5px solid ${C.bdr}`, borderRadius: 3, background: "none", cursor: "pointer", flexShrink: 0 }} />
                <span style={{ color: C.dim }}>{table.edges[i].toFixed(1)} –</span>
                <input type="number" value={table.edges[i + 1]} onChange={e => setEdge(i, e.target.value)} style={{ ...iSt, width: 54 }} />
                <input value={table.labels[i]} onChange={e => setLabel(i, e.target.value)} placeholder="libellé" style={{ ...iSt, width: "auto", flex: 1, fontFamily: F, fontSize: 9.5 }} />
                <button onClick={() => delRow(i)} title="Supprimer" style={{ background: "none", border: "none", color: "#e11d1d", cursor: "pointer", fontSize: 12, flexShrink: 0 }}>×</button>
              </div>
            ); })}
          </div>
          <div style={{ display: "flex", gap: 6 }}>
            <button onClick={addRow} style={{ fontFamily: F, fontSize: 10, padding: "5px 10px", borderRadius: 5, cursor: "pointer", background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.mut }}>+ Classe</button>
            <button onClick={applyManual} disabled={loading} style={{ fontFamily: F, fontSize: 10, fontWeight: 600, padding: "5px 0", borderRadius: 5, flex: 1, cursor: loading ? "default" : "pointer", background: C.acc, color: "#fff", border: "none", opacity: loading ? 0.6 : 1 }}>{loading ? "Rendu…" : "Appliquer les classes"}</button>
          </div>
        </>
      ) : (
        <div style={{ fontSize: 9, color: C.dim }}>Choisissez une méthode et « Classer » pour éditer les classes.</div>
      )}

      {status && (<div style={{ fontSize: 9, padding: "3px 6px", borderRadius: 4, background: (status.type === "ok" ? C.acc : C.red) + "15", color: status.type === "ok" ? C.acc : C.red, border: `0.5px solid ${(status.type === "ok" ? C.acc : C.red)}44` }}>{status.msg}</div>)}
    </div>
  );
}

// ── Sémiologie d'un nuage de points LiDAR (couleur, filtre classe, légende) ──
const _pcBtn = (C) => ({ fontFamily: F, fontSize: 8, padding: "1px 6px", borderRadius: 4, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim, cursor: "pointer" });
function PointcloudStylePanel({ layer, mapRef }) {
  const C = useThemeContext();
  const [, force] = useState(0);
  const pc = getPC(layer.id);
  if (!pc) return <div style={{ fontSize: 9, color: C.dim, padding: "4px 0" }}>Nuage indisponible — rechargez-le depuis le menu LiDAR.</div>;
  const getMap = () => mapRef?.current?.getMap?.() || null;
  const st = pc.style;
  const hist = pc.histogram || {};
  const entries = Object.entries(hist).sort((a, b) => b[1] - a[1]);
  const total = entries.reduce((s, [, c]) => s + c, 0) || 1;
  const allNums = entries.map(([c]) => Number(c));
  const sel = st.classSel || new Set(allNums);
  const hasClass = !!pc.full.classification;
  const hasRgb = !!pc.full.rgb;
  const apply = async (patch) => { await applyPCStyle(getMap(), layer.id, patch); force(x => x + 1); };
  const toggle = (n) => { const nx = new Set(sel); if (nx.has(n)) nx.delete(n); else nx.add(n); apply({ classSel: nx }); };
  const inp = { fontFamily: M, fontSize: 10, padding: "4px 6px", borderRadius: 5, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box", cursor: "pointer" };
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 7, padding: "6px 0 2px" }}>
      <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Sémiologie nuage</div>
      <div>
        <div style={{ fontSize: 8, color: C.dim, marginBottom: 2 }}>Variable de couleur</div>
        <select value={st.colorMode} onChange={e => apply({ colorMode: e.target.value })} style={inp}>
          {hasClass && <option value="class">Classification</option>}
          {hasRgb && <option value="rgb">Couleur RGB</option>}
          <option value="elevation">Élévation (Z)</option>
          <option value="uniform">Uniforme</option>
        </select>
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span style={{ fontSize: 9, color: C.dim, flexShrink: 0 }}>Taille point</span>
        <input type="range" min="1" max="8" step="0.5" value={st.pointSize} onChange={e => apply({ pointSize: parseFloat(e.target.value) })} style={{ flex: 1, height: 3 }} />
        <span style={{ fontFamily: M, fontSize: 9, color: C.txt }}>{st.pointSize}</span>
      </div>
      {st.colorMode === "class" && entries.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span style={{ fontSize: 8, color: C.dim }}>Classes — cocher pour afficher · pastille = couleur</span>
            <div style={{ display: "flex", gap: 4 }}>
              <button onClick={() => apply({ classSel: new Set(allNums) })} style={_pcBtn(C)}>Tout</button>
              <button onClick={() => apply({ classSel: new Set() })} style={_pcBtn(C)}>Aucun</button>
            </div>
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 2, maxHeight: 160, overflowY: "auto" }}>
            {entries.map(([cls, cnt]) => {
              const n = Number(cls);
              const info = ASPRS_CLASSES[cls] || [`Classe ${cls}`, "#bdbdbd"];
              const hex = st.classOverrides[cls] || info[1];
              const on = sel.has(n);
              return (
                <div key={cls} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 9, opacity: on ? 1 : 0.4 }}>
                  <input type="checkbox" checked={on} onChange={() => toggle(n)} style={{ accentColor: C.acc, cursor: "pointer" }} />
                  <label title="Changer la couleur" style={{ width: 14, height: 14, borderRadius: 3, background: hex, flexShrink: 0, border: "0.5px solid rgba(0,0,0,.2)", cursor: "pointer", position: "relative", overflow: "hidden" }}>
                    <input type="color" value={hex} onChange={e => apply({ classOverrides: { ...st.classOverrides, [cls]: e.target.value } })} style={{ position: "absolute", inset: -4, opacity: 0, cursor: "pointer", border: "none", padding: 0 }} />
                  </label>
                  <span style={{ color: C.mut, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{info[0]} <span style={{ color: C.dim, fontFamily: M }}>· cl.{cls}</span></span>
                  <span style={{ color: C.dim, fontFamily: M }}>{(cnt / total * 100).toFixed(0)}%</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Légende + restyle couleur pour couches de classification ──
function ClassifLegendPanel({ layer, onUpdateRasterLayer, C }) {
  const entries = layer.legend || [];

  const [colors,  setColors]  = useState(() => entries.map(e => e.color || "#888888"));
  const [loading, setLoading] = useState(false);
  const [status,  setStatus]  = useState(null);

  // Synchronise si la légende change après un restyle externe
  useEffect(() => {
    setColors((layer.legend || []).map(e => e.color || "#888888"));
  }, [layer.legend]);

  if (!entries.length) return null;

  // Deux familles de couches classées, deux façons de restyler :
  //  • classification SUPERVISÉE (ClassifSupPanel) → job_id serveur + /classify/restyle
  //  • indicateur GEE classé en quantiles/Jenks (IndicatorModal) → pas de job_id,
  //    on rejoue /api/gee/tiles avec les mêmes params + la palette forcée.
  const applyRestyle = async () => {
    const gp = layer._geeParams;
    if (!layer.job_id && !gp) {
      setStatus({ type: "error", msg: "Couche non restylable — rechargez-la" });
      return;
    }
    setLoading(true);
    setStatus(null);
    try {
      let tileUrl, newLegend, visParams;

      if (layer.job_id) {
        const res = await fetch(`${API}/api/gee/classify/restyle`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ job_id: layer.job_id, class_colors: colors }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);
        tileUrl   = data.tile_url;
        newLegend = entries.map((e, i) => ({ ...e, color: colors[i] ?? e.color }));
      } else {
        const res = await fetch(`${API}/api/gee/tiles`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            ...gp,
            // n_classes recalé sur la légende affichée : le serveur peut avoir
            // fusionné des ruptures identiques (données peu variées).
            n_classes: colors.length,
            vis_params_override: { palette: colors.map(c => c.replace("#", "")) },
          }),
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);
        tileUrl   = data.tile_url;
        newLegend = data.legend || entries.map((e, i) => ({ ...e, color: colors[i] ?? e.color }));
        visParams = data.vis_params || null;
      }

      // Met à jour la légende locale + la tuile MapLibre via le callback App.jsx
      onUpdateRasterLayer?.(layer.id, {
        tileUrl, legend: newLegend, ...(visParams ? { visParams } : {}),
      });
      setStatus({ type: "ok", msg: "✓ Couleurs appliquées" });
    } catch (e) {
      setStatus({ type: "error", msg: e.message });
    }
    setLoading(false);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 5, padding: "6px 0 2px" }}>

      {/* En-tête */}
      <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>
        Légende — classification
      </div>

      {/* Liste des classes */}
      {entries.map((entry, i) => (
        <div key={entry.class_id ?? i}
          style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <input
            type="color"
            value={colors[i] || "#888888"}
            onChange={e => {
              const next = [...colors];
              next[i] = e.target.value;
              setColors(next);
            }}
            style={{
              width: 22, height: 18, border: "none", borderRadius: 3,
              cursor: "pointer", background: "none", padding: 0, flexShrink: 0,
            }}
          />
          {/* Pastille couleur actuelle */}
          <div style={{
            width: 10, height: 10, borderRadius: 2, flexShrink: 0,
            background: colors[i] || entry.color || "#888",
            border: `0.5px solid ${C.bdr}`,
          }} />
          <span style={{
            fontSize: 10, color: C.txt, flex: 1, minWidth: 0,
            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          }}>
            {entry.label || `Classe ${entry.class_id}`}
          </span>
          <span style={{ fontSize: 9, color: C.dim, fontFamily: M, flexShrink: 0 }}>
            {entry.class_id}
          </span>
        </div>
      ))}

      {/* Message statut */}
      {status && (
        <div style={{
          fontSize: 9, padding: "3px 6px", borderRadius: 4,
          background: (status.type === "ok" ? C.acc : C.red) + "15",
          color:      status.type === "ok" ? C.acc : C.red,
          border:     `0.5px solid ${(status.type === "ok" ? C.acc : C.red)}44`,
        }}>
          {status.msg}
        </div>
      )}

      {/* Bouton Appliquer */}
      <button
        onClick={applyRestyle}
        disabled={loading}
        style={{
          fontFamily: F, fontSize: 10, fontWeight: 600, padding: "6px 0",
          borderRadius: 5, width: "100%",
          cursor:     loading ? "default" : "pointer",
          background: loading ? C.hover : C.acc,
          color:      loading ? C.dim : "#fff",
          border:     "none",
          opacity:    loading ? 0.6 : 1,
          transition: "background .15s",
        }}
      >
        {loading ? "Mise à jour GEE…" : "Appliquer les couleurs"}
      </button>
    </div>
  );
}

// presse-papier de style (copier/coller la symbologie d'une couche à l'autre)
let STYLE_CLIP = null;

// Libellé de géométrie pour le badge de la fenêtre (POLYGONES / LIGNES / POINTS).
function geomLabel(l) {
  if (l.isRaster) return "RASTER";
  const t = (l.geojson?.features || []).find(f => f?.geometry)?.geometry?.type || "";
  if (/Polygon/i.test(t)) return "POLYGONES";
  if (/LineString/i.test(t)) return "LIGNES";
  if (/Point/i.test(t)) return "POINTS";
  return "VECTEUR";
}

// Emprise (bbox) calculée depuis les coordonnées du GeoJSON → [minX, minY, maxX, maxY].
function bboxOf(gj) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const walk = (c) => {
    if (typeof c[0] === "number") { const [x, y] = c; if (x < minX) minX = x; if (y < minY) minY = y; if (x > maxX) maxX = x; if (y > maxY) maxY = y; return; }
    for (const k of c) walk(k);
  };
  (gj?.features || []).forEach(f => { if (f?.geometry?.coordinates) walk(f.geometry.coordinates); });
  return isFinite(minX) ? [minX, minY, maxX, maxY] : null;
}

// ── Fenêtre « Symbologie » flottante : déplaçable (par le bandeau, dans toutes les
//    directions) + redimensionnable (poignée en bas à droite) + réductible. ────────
function PropsWindow({ title, isRaster, badge, z, onFocus, onClose, children }) {
  const C = useThemeContext();
  const winRef = useRef(null);
  const [isMobile, setIsMobile] = useState(() => typeof window !== "undefined" && window.innerWidth < 640);
  const [pos, setPos] = useState(() => ({ x: Math.max(10, Math.min((window.innerWidth || 1200) - 494, 260)), y: 80 }));
  const [size, setSize] = useState({ w: 470, h: 520 });
  const [min, setMin] = useState(false);
  const act = useRef(null);   // { mode, sx, sy, px, py, w, h }
  const minW = 300, minH = 200;

  useEffect(() => {
    const onR = () => setIsMobile(window.innerWidth < 640);
    window.addEventListener("resize", onR);
    return () => window.removeEventListener("resize", onR);
  }, []);

  // Au montage (desktop) : borner la position dans le viewport.
  useEffect(() => {
    if (isMobile) return;
    const el = winRef.current; if (!el) return;
    const w = el.offsetWidth, h = el.offsetHeight;
    setPos(p => ({ x: Math.max(6, Math.min(p.x, Math.max(6, window.innerWidth - w - 6))), y: Math.max(6, Math.min(p.y, Math.max(6, window.innerHeight - h - 6))) }));
  }, [isMobile]);

  useEffect(() => {
    const move = (e) => {
      const a = act.current; if (!a) return;
      const dx = e.clientX - a.sx, dy = e.clientY - a.sy;
      if (a.mode === "drag") {
        const el = winRef.current, w = el?.offsetWidth || size.w, h = el?.offsetHeight || minH;
        setPos({ x: Math.max(6, Math.min(a.px + dx, Math.max(6, window.innerWidth - w - 6))), y: Math.max(6, Math.min(a.py + dy, Math.max(6, window.innerHeight - h - 6))) });
        return;
      }
      let x = a.px, y = a.py, w = a.w, h = a.h; const m = a.mode;
      if (m.includes("e")) w = Math.max(minW, a.w + dx);
      if (m.includes("s")) h = Math.max(minH, a.h + dy);
      if (m.includes("w")) { w = Math.max(minW, a.w - dx); x = a.px + (a.w - w); }
      if (m.includes("n")) { h = Math.max(minH, a.h - dy); y = a.py + (a.h - h); }
      setPos({ x, y }); setSize({ w, h });
    };
    const up = () => { if (act.current) { act.current = null; document.body.style.userSelect = ""; } };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
    return () => { window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", up); };
  }, [size.w]);

  const begin = (mode) => (e) => {
    if (mode === "drag" && e.target.closest("[data-nodrag]")) return;
    e.preventDefault(); onFocus?.();
    const rect = winRef.current?.getBoundingClientRect();
    act.current = { mode, sx: e.clientX, sy: e.clientY, px: pos.x, py: pos.y, w: size.w ?? rect?.width ?? minW, h: size.h ?? rect?.height ?? minH };
    document.body.style.userSelect = "none";
  };

  const hbtn = { background: "none", border: "none", cursor: "pointer", color: C.dim, lineHeight: 0, padding: "3px 5px", borderRadius: 5, display: "flex", alignItems: "center" };
  const H = { position: "absolute", zIndex: 12, touchAction: "none" };
  const handles = [
    { m: "n",  s: { top: -3, left: 16, right: 16, height: 9, cursor: "ns-resize" } },
    { m: "s",  s: { bottom: -3, left: 16, right: 16, height: 9, cursor: "ns-resize" } },
    { m: "w",  s: { left: -3, top: 16, bottom: 16, width: 9, cursor: "ew-resize" } },
    { m: "e",  s: { right: -3, top: 16, bottom: 16, width: 9, cursor: "ew-resize" } },
    { m: "nw", s: { top: -4, left: -4, width: 16, height: 16, cursor: "nwse-resize" } },
    { m: "ne", s: { top: -4, right: -4, width: 16, height: 16, cursor: "nesw-resize" } },
    { m: "sw", s: { bottom: -4, left: -4, width: 16, height: 16, cursor: "nesw-resize" } },
    { m: "se", s: { bottom: -4, right: -4, width: 16, height: 16, cursor: "nwse-resize" } },
  ];
  const frame = isMobile
    ? { position: "fixed", left: 8, right: 8, bottom: 8, top: "auto", width: "auto", height: min ? "auto" : "82vh", maxWidth: "none" }
    : { position: "fixed", left: pos.x, top: pos.y, width: size.w, height: min ? "auto" : size.h, maxWidth: "96vw", maxHeight: "92vh" };

  return createPortal(
    <div ref={winRef} onPointerDown={() => onFocus?.()} style={{ ...frame, zIndex: z || 1400,
      background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 14,
      boxShadow: "0 16px 48px rgba(0,0,0,.4)", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      {!isMobile && !min && handles.map(h => <div key={h.m} onPointerDown={begin(h.m)} style={{ ...H, ...h.s,
        ...(h.m === "se" ? { background: `repeating-linear-gradient(-45deg, ${C.dim} 0 1.2px, transparent 1.2px 3px)`, opacity: 0.7, borderBottomRightRadius: 9 } : {}) }} />)}
      <div onPointerDown={isMobile ? undefined : begin("drag")} style={{ display: "flex", alignItems: "center", gap: 8, padding: "9px 11px",
        borderBottom: `0.5px solid ${C.bdr}`, cursor: isMobile ? "default" : "move", userSelect: "none", touchAction: isMobile ? "auto" : "none", flexShrink: 0 }}>
        <span style={{ display: "flex", color: isRaster ? "#a06bd6" : C.acc, flexShrink: 0 }}><IcPalette size={14} /></span>
        <span style={{ fontFamily: M, fontSize: 8.5, fontWeight: 700, letterSpacing: ".03em", padding: "2px 5px", borderRadius: 5,
          color: isRaster ? "#a06bd6" : C.acc, border: `1px solid ${isRaster ? "#a06bd655" : C.acc + "55"}`, background: (isRaster ? "#a06bd6" : C.acc) + "18", flexShrink: 0 }}>{badge || (isRaster ? "RASTER" : "VECTEUR")}</span>
        <span style={{ fontFamily: F, fontWeight: 600, fontSize: 12.5, color: C.txt, flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{title}</span>
        <button data-nodrag onClick={() => setMin(m => !m)} title={min ? "Agrandir" : "Réduire"} style={hbtn}>{min ? <IcMaximize size={13} /> : <IcMinus size={14} />}</button>
        <button data-nodrag onClick={onClose} title="Fermer" style={hbtn}><IcX size={14} /></button>
      </div>
      {!min && <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", overflow: "hidden" }}>{children}</div>}
    </div>,
    document.body
  );
}

// ── Table attributaire (onglet Attributs) : colonnes = champs, lignes = entités ──
function AttributeTable({ layer, C }) {
  const [q, setQ] = useState("");
  const feats = layer.geojson?.features || [];
  if (!feats.length) return <div style={{ fontSize: 11, color: C.dim }}>Aucune entité.</div>;
  const cols = [], seen = new Set();
  feats.slice(0, 300).forEach(f => Object.keys(f.properties || {}).forEach(k => {
    if (!seen.has(k) && !["geom_json", "geom_wkt"].includes(k)) { seen.add(k); cols.push(k); }
  }));
  const ql = q.trim().toLowerCase();
  const rows = (ql ? feats.filter(f => Object.values(f.properties || {}).some(v => String(v).toLowerCase().includes(ql))) : feats).slice(0, 1000);
  const th = { position: "sticky", top: 0, background: C.hover, color: C.dim, fontSize: 9.5, textTransform: "uppercase", letterSpacing: ".04em", fontWeight: 600, textAlign: "left", padding: "5px 8px", borderBottom: `0.5px solid ${C.bdr}`, whiteSpace: "nowrap" };
  const td = { padding: "4px 8px", fontSize: 10.5, color: C.txt, borderBottom: `0.5px solid ${C.bdr}`, whiteSpace: "nowrap", maxWidth: 220, overflow: "hidden", textOverflow: "ellipsis" };
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 7, minHeight: 0 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <input value={q} onChange={e => setQ(e.target.value)} placeholder="Rechercher dans les attributs…"
          style={{ flex: 1, fontFamily: F, fontSize: 11, padding: "5px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }} />
        <span style={{ fontSize: 10, color: C.dim, fontFamily: M, whiteSpace: "nowrap" }}>{rows.length}/{feats.length}</span>
      </div>
      <div style={{ overflow: "auto", border: `0.5px solid ${C.bdr}`, borderRadius: 8, maxHeight: 380 }}>
        <table style={{ borderCollapse: "collapse", width: "100%", fontFamily: M }}>
          <thead><tr><th style={{ ...th, width: 34, textAlign: "right" }}>#</th>{cols.map(c => <th key={c} style={th}>{c}</th>)}</tr></thead>
          <tbody>
            {rows.map((f, i) => (
              <tr key={i}>
                <td style={{ ...td, color: C.dim, textAlign: "right" }}>{i + 1}</td>
                {cols.map(c => { const v = f.properties?.[c]; return (
                  <td key={c} style={{ ...td, textAlign: typeof v === "number" ? "right" : "left" }}>
                    {v == null ? "" : typeof v === "number" ? v.toLocaleString("fr") : String(v)}
                  </td>
                ); })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {feats.length > 1000 && <div style={{ fontSize: 9.5, color: C.dim }}>Affichage limité à 1000 entités sur {feats.length}.</div>}
    </div>
  );
}

// ── Onglet BIVARIÉ : croise 2 variables numériques en une matrice 3×3 ────────────
function BivariateStylePanel({ layer, onStyle, C }) {
  const nums = getLayerAttrs(layer).num;
  const [ax, setAx]  = useState(layer.biv?.attrX || "");
  const [ay, setAy]  = useState(layer.biv?.attrY || "");
  const [pal, setPal] = useState(layer.biv?.paletteKey || Object.keys(BIVARIATE_PALETTES)[0]);
  const on = !!layer.biv;
  const apply = (x, y, p) => {
    if (!x || !y) { onStyle(layer.id, { biv: null }); return; }
    onStyle(layer.id, { biv: buildBivariate(layer, { attrX: x, attrY: y, palette: p }) });
  };
  const selSt = { fontFamily: F, fontSize: 11, padding: "5px 7px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%" };
  const lbl = { fontSize: 9.5, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 4, fontWeight: 600 };
  if (nums.length < 2) return <div style={{ fontSize: 11, color: C.dim }}>Il faut au moins 2 champs numériques pour un style bivarié.</div>;
  const cols = BIVARIATE_PALETTES[pal] || [];
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 11 }}>
      <div style={{ fontSize: 11, color: C.mut, lineHeight: 1.5 }}>Croise <b style={{ color: C.txt }}>deux variables</b> en une matrice 3×3 (tertiles). Chaque entité prend la couleur du croisement A × B.</div>
      <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
        <div style={{ flex: 1, minWidth: 118 }}>
          <div style={lbl}>Variable A (vert.)</div>
          <select value={ax} onChange={e => { setAx(e.target.value); apply(e.target.value, ay, pal); }} style={selSt}>
            <option value="">-- Choisir --</option>{nums.map(a => <option key={a} value={a}>{a}</option>)}
          </select>
        </div>
        <div style={{ flex: 1, minWidth: 118 }}>
          <div style={lbl}>Variable B (horiz.)</div>
          <select value={ay} onChange={e => { setAy(e.target.value); apply(ax, e.target.value, pal); }} style={selSt}>
            <option value="">-- Choisir --</option>{nums.map(a => <option key={a} value={a}>{a}</option>)}
          </select>
        </div>
      </div>
      <div>
        <div style={lbl}>Palette bivariée</div>
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
          {Object.entries(BIVARIATE_PALETTES).map(([name, p]) => (
            <button key={name} onClick={() => { setPal(name); apply(ax, ay, name); }} title={name}
              style={{ padding: 3, borderRadius: 7, cursor: "pointer", background: "transparent", border: pal === name ? `2px solid ${C.acc}` : "2px solid transparent" }}>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3,10px)", gridTemplateRows: "repeat(3,10px)", gap: 1 }}>
                {[6, 7, 8, 3, 4, 5, 0, 1, 2].map(i => <span key={i} style={{ width: 10, height: 10, background: p[i], borderRadius: 1 }} />)}
              </div>
            </button>
          ))}
        </div>
      </div>
      {on && (
        <div style={{ display: "flex", alignItems: "flex-end", gap: 8, paddingTop: 2 }}>
          <span style={{ fontSize: 9, color: C.dim, writingMode: "vertical-rl", transform: "rotate(180deg)", whiteSpace: "nowrap" }}>{ax || "A"} →</span>
          <div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3,20px)", gridTemplateRows: "repeat(3,20px)", gap: 2 }}>
              {[2, 1, 0].flatMap(a => [0, 1, 2].map(b => <span key={a * 3 + b} style={{ width: 20, height: 20, background: cols[a * 3 + b], borderRadius: 3, border: "0.5px solid rgba(0,0,0,.12)" }} />))}
            </div>
            <div style={{ fontSize: 9, color: C.dim, marginTop: 2 }}>{ay || "B"} →</div>
          </div>
        </div>
      )}
      {on && (
        <button onClick={() => onStyle(layer.id, { biv: null })} style={{ alignSelf: "flex-start", fontFamily: F, fontSize: 11, padding: "5px 10px", borderRadius: 6, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.mut, cursor: "pointer" }}>Désactiver le bivarié</button>
      )}
    </div>
  );
}

// ── Contenu de la fenêtre Symbologie : structure à ONGLETS (façon QGIS / maquette
//    validée), qui réutilise les vrais composants de style existants. ─────────────
function LayerSymbology({ l, geomLabel, onClose, onStyle, onClassify, onExport, onExportFmt, onRemove, onUpdateRasterLayer, onUpdateGeojson, mapRef, openStats, exportImageTiff, tiffBusy }) {
  const C = useThemeContext();
  const isR = l.isRaster, isVec = !l.isRaster && !!l.geojson;
  const TABS = isR
    ? [["sym", "Symbologie", IcPalette], ["render", "Rendu", IcSliders], ["fields", "Champs", IcTable], ["info", "Infos", IcInfo]]
    : [["sym", "Symbologie", IcPalette], ["biv", "Bivarié", IcVenn], ["graphs", "Graphiques", IcBarChart], ["lab", "Étiquettes", IcHash], ["attr", "Attributs", IcTable], ["render", "Rendu", IcSliders], ["fields", "Champs", IcEdit], ["info", "Infos", IcInfo]];
  const [tab, setTab] = useState("sym");
  const [expMenu, setExpMenu] = useState(false);
  const [, force] = useState(0);
  const numAttrs = new Set(), txtAttrs = new Set();
  (l.geojson?.features || []).slice(0, 20).forEach(f => Object.entries(f.properties || {}).forEach(([k, v]) => {
    if (typeof v === "number" && !["id"].includes(k)) numAttrs.add(k);
    if (v != null && v !== "" && !["id", "geom_json"].includes(k)) txtAttrs.add(k);
  }));
  const sub = { fontSize: 10, textTransform: "uppercase", letterSpacing: ".05em", color: C.dim, fontWeight: 600, margin: "2px 0 2px" };
  const selSt = { fontFamily: F, fontSize: 11, padding: "4px 6px", borderRadius: 5, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", flex: 1 };
  const rowSt = { display: "flex", alignItems: "center", gap: 8, fontSize: 11 };
  const dim = { color: C.dim }, val = { color: C.dim, fontFamily: M };

  // Copier / Coller le style d'une couche à l'autre (presse-papier de module)
  const copyStyle  = () => { STYLE_CLIP = { color: l.color, outlineColor: l.outlineColor, radius: l.radius, strokeWidth: l.strokeWidth, markerShape: l.markerShape, opacity: l.opacity, classCfg: l.classCfg ? { ...l.classCfg } : null }; force(x => x + 1); };
  const pasteStyle = () => { if (!STYLE_CLIP) return; const { classCfg, ...st } = STYLE_CLIP; onStyle(l.id, st); onClassify(l.id, classCfg || null); };
  const footBtn = (Icon, label, onClick, opts = {}) => (
    <button onClick={onClick} disabled={opts.disabled} title={label} style={{
      fontFamily: F, fontSize: 11, fontWeight: 600, padding: "6px 10px", borderRadius: 7,
      display: "inline-flex", alignItems: "center", gap: 5, cursor: opts.disabled ? "default" : "pointer",
      background: opts.primary ? C.acc : "transparent",
      color: opts.primary ? "#fff" : opts.danger ? C.red : C.mut,
      border: `0.5px solid ${opts.primary ? C.acc : C.bdr}`, opacity: opts.disabled ? 0.45 : 1,
    }}><Icon size={13} /> {label}</button>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }}>
      {/* corps : rail d'onglets à GAUCHE + contenu à droite (façon maquette QGIS) */}
      <div style={{ flex: 1, minHeight: 0, display: "flex" }}>
        <div style={{ width: 116, flexShrink: 0, minHeight: 0, borderRight: `1px solid ${C.bdr}`, background: C.bg2 || C.hover, padding: "9px 7px",
          display: "flex", flexDirection: "column", gap: 2, overflowY: "auto" }}>
          {TABS.map(([k, la, Icon]) => (
            <button key={k} onClick={() => setTab(k)} style={{
              display: "flex", alignItems: "center", gap: 8, padding: "8px 9px", borderRadius: 7, border: "none",
              cursor: "pointer", width: "100%", textAlign: "left", fontFamily: F, fontSize: 11.5, fontWeight: tab === k ? 600 : 400,
              background: tab === k ? C.card : "transparent", color: tab === k ? C.acc : C.dim,
              boxShadow: tab === k ? `inset 2px 0 0 ${C.acc}` : "none",
            }}>
              <span style={{ display: "flex", color: tab === k ? C.acc : C.dim, flexShrink: 0 }}><Icon size={14} /></span> {la}
            </button>
          ))}
        </div>

        <div style={{ flex: 1, minWidth: 0, minHeight: 0, overflowY: "auto", padding: "12px", display: "flex", flexDirection: "column", gap: 10 }}>
        {tab === "sym" && (<>
          {isR && l.visParams && <RasterStylePanel layer={l} onUpdateLayer={(id, u) => onUpdateRasterLayer?.(id, u)} />}
          {l.kind === "pointcloud" && <PointcloudStylePanel layer={l} mapRef={mapRef} />}
          {l.kind === "image" && l.rasterToken && <RasterImageStylePanel layer={l} onUpdate={(id, u) => onUpdateRasterLayer?.(id, u)} />}
          {isR && l.legend?.length > 0 && <ClassifLegendPanel layer={l} onUpdateRasterLayer={onUpdateRasterLayer} C={C} />}
          {isVec && (
            <ClassPanel key={`${l.id}-${l.classCfg?.ramp}-${l.classCfg?.type}`} layer={l} classification={l.classCfg}
              onChange={cfg => onClassify(l.id, cfg)} onStyle={onStyle} mapRef={mapRef} />
          )}
        </>)}

        {/* Style bivarié — croise 2 variables en matrice 3×3 */}
        {tab === "biv" && isVec && <BivariateStylePanel layer={l} onStyle={onStyle} C={C} />}

        {/* Graphiques par entité — SUPERPOSABLES à une classification (aplat + camembert/ronds) */}
        {tab === "graphs" && isVec && (
          <ChartStyleBlock layer={l} cfg={l.chartCfg} onChange={cfg => onStyle(l.id, { chartCfg: cfg })} mapRef={mapRef}
            layerOpacity={l.opacity ?? 1} onLayerOpacity={o => onStyle(l.id, { opacity: o })} />
        )}

        {tab === "lab" && isVec && (<>
          <label style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 11.5, color: C.txt, cursor: "pointer" }}>
            <input type="checkbox" checked={!!l.labels} onChange={() => onStyle(l.id, { labels: !l.labels })} /> Afficher les étiquettes
          </label>
          {l.labels && txtAttrs.size > 0 && (
            <div style={rowSt}>
              <span style={dim}>Étiqueter avec</span>
              <select value={l.labelAttr || "name"} onChange={e => onStyle(l.id, { labelAttr: e.target.value })} style={selSt}>
                {[...txtAttrs].map(a => <option key={a} value={a}>{a}</option>)}
              </select>
            </div>
          )}
          {l.labels && (
            <>
              <div style={rowSt}>
                <span style={dim}>Taille</span>
                <input type="range" min="8" max="28" step="1" value={l.labelSize || 11} onChange={e => onStyle(l.id, { labelSize: parseInt(e.target.value) })} style={{ flex: 1, height: 3 }} />
                <span style={val}>{l.labelSize || 11} px</span>
              </div>
              <div style={rowSt}>
                <span style={dim}>Halo</span>
                <input type="range" min="0" max="4" step="0.5" value={l.labelHalo ?? 1.5} onChange={e => onStyle(l.id, { labelHalo: parseFloat(e.target.value) })} style={{ flex: 1, height: 3 }} />
                <span style={val}>{l.labelHalo ?? 1.5} px · blanc</span>
              </div>
              <label style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 11.5, color: C.txt, cursor: "pointer" }}>
                <input type="checkbox" checked={l.labelAvoidOverlap ?? true} onChange={() => onStyle(l.id, { labelAvoidOverlap: !(l.labelAvoidOverlap ?? true) })} /> Éviter les chevauchements
              </label>
            </>
          )}
          {l.labels && txtAttrs.size === 0 && <div style={{ fontSize: 10.5, color: C.dim }}>Aucun attribut texte à afficher.</div>}
        </>)}

        {tab === "render" && (<>
          <div style={rowSt}>
            <span style={dim}>Opacité</span>
            <input type="range" min="0" max="1" step="0.05" value={l.opacity} onChange={e => onStyle(l.id, { opacity: parseFloat(e.target.value) })} style={{ flex: 1, height: 3 }} />
            <span style={val}>{Math.round(l.opacity * 100)}%</span>
          </div>
          {isVec && (
            <div style={rowSt}>
              <span style={dim}>Visible de zoom</span>
              <input type="number" min="0" max="24" placeholder="0" value={l.minZoom ?? ""} onChange={e => onStyle(l.id, { minZoom: e.target.value === "" ? null : parseFloat(e.target.value) })} style={{ ...selSt, flex: "none", width: 52 }} />
              <span style={{ color: C.dim }}>→</span>
              <input type="number" min="0" max="24" placeholder="24" value={l.maxZoom ?? ""} onChange={e => onStyle(l.id, { maxZoom: e.target.value === "" ? null : parseFloat(e.target.value) })} style={{ ...selSt, flex: "none", width: 52 }} />
            </div>
          )}
          {isVec && (
            <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
              <Btn small color={C.amb} active={l.heatmap} onClick={() => onStyle(l.id, { heatmap: !l.heatmap, extrude: false })}>Heatmap</Btn>
              <Btn small color={C.blu} active={l.extrude} onClick={() => onStyle(l.id, { extrude: !l.extrude, heatmap: false })}>3D</Btn>
              <Btn small color={C.pnk} active={l.cluster} onClick={() => onStyle(l.id, { cluster: !l.cluster })}>Cluster</Btn>
            </div>
          )}
          {isVec && l.extrude && numAttrs.size > 0 && (
            <div style={rowSt}>
              <span style={dim}>Hauteur</span>
              <select value={l.extrudeAttr || ""} onChange={e => onStyle(l.id, { extrudeAttr: e.target.value })} style={selSt}>
                <option value="">auto (height)</option>{[...numAttrs].map(a => <option key={a} value={a}>{a}</option>)}
              </select>
              <input type="range" min="1" max="20" step="1" value={l.extrudeScale || 1} onChange={e => onStyle(l.id, { extrudeScale: parseInt(e.target.value) })} style={{ width: 50, height: 3 }} />
              <span style={val}>{l.extrudeScale || 1}x</span>
            </div>
          )}
          {isR && openStats && l._geeParams?.dataset && l._geeParams?.index && (
            <button onClick={e => openStats(e, l)} style={{ fontFamily: F, fontSize: 10, padding: "6px 0", borderRadius: 5, width: "100%", background: "transparent", border: `0.5px solid ${C.acc}`, color: C.acc, cursor: "pointer", fontWeight: 500, display: "flex", alignItems: "center", justifyContent: "center", gap: 6 }}><IcBarChart size={13} /> Statistiques de la zone</button>
          )}
        </>)}

        {tab === "attr" && isVec && <AttributeTable layer={l} C={C} />}

        {tab === "fields" && (<>
          {isVec && (<>
            <div style={sub}>Champs — variables</div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 5 }}>
              {[...numAttrs].map(a => (
                <span key={a} title="Numérique" style={{ display: "inline-flex", alignItems: "center", gap: 5, fontFamily: M, fontSize: 10.5, color: C.txt, background: C.hover, border: `0.5px solid ${C.bdr}`, borderRadius: 5, padding: "3px 7px" }}>
                  <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.blu }} />{a}
                </span>
              ))}
              {[...txtAttrs].filter(a => !numAttrs.has(a)).map(a => (
                <span key={a} title="Texte" style={{ display: "inline-flex", alignItems: "center", gap: 5, fontFamily: M, fontSize: 10.5, color: C.txt, background: C.hover, border: `0.5px solid ${C.bdr}`, borderRadius: 5, padding: "3px 7px" }}>
                  <span style={{ width: 6, height: 6, borderRadius: "50%", background: C.mut }} />{a}
                </span>
              ))}
            </div>
            <div style={{ fontSize: 9.5, color: C.dim }}>● {numAttrs.size} numérique(s) · ● {[...txtAttrs].filter(a => !numAttrs.has(a)).length} texte</div>
            <div style={{ ...sub, marginTop: 8 }}>Calculateur de champ</div>
            <FieldCalcBlock layer={l} onApply={(gj, col) => onUpdateGeojson?.(l.id, gj, col)} />
          </>)}
          {isR && <div style={{ fontSize: 11.5, color: C.mut }}>Couche raster — palette et classes dans l'onglet Symbologie.</div>}
        </>)}

        {tab === "info" && (() => {
          const bb = isVec ? bboxOf(l.geojson) : (l.bbox || l._geeParams?.bbox || null);
          const fc = n => (typeof n === "number" ? n.toFixed(2) : n);
          const crs = l.crs || l.epsg || l._geeParams?.crs || "EPSG:4326 · WGS 84";
          const dd = { margin: 0, color: C.txt }, ddM = { margin: 0, fontFamily: M, color: C.txt };
          return (
            <dl style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: "7px 14px", fontSize: 12, margin: 0 }}>
              <dt style={dim}>Nom</dt><dd style={dd}>{l.name}</dd>
              <dt style={dim}>Type</dt><dd style={dd}>{isR ? "Raster" : `Vecteur — ${(geomLabel || "").toLowerCase()}`}</dd>
              {l.geojson && (<><dt style={dim}>Entités</dt><dd style={ddM}>{(l.geojson.features || []).length}</dd></>)}
              {bb && (<><dt style={dim}>Emprise</dt><dd style={ddM}>{fc(bb[0])}, {fc(bb[1])} → {fc(bb[2])}, {fc(bb[3])}</dd></>)}
              <dt style={dim}>SCR</dt><dd style={ddM}>{crs}</dd>
            </dl>
          );
        })()}
        </div>
      </div>

      {/* barre d'actions façon maquette : Copier · Coller · Exporter ▾ · Réinit. · Supprimer · OK */}
      <div style={{ position: "relative", borderTop: `0.5px solid ${C.bdr}`, padding: "9px 12px", display: "flex",
        gap: 6, flexWrap: "wrap", alignItems: "center", background: C.card }}>
        {footBtn(IcCopy, "Copier", copyStyle, { disabled: !isVec })}
        {footBtn(IcClipboard, "Coller", pasteStyle, { disabled: !isVec || !STYLE_CLIP })}
        <div style={{ position: "relative" }}>
          {footBtn(IcFileDown, "Exporter ▾", () => setExpMenu(m => !m))}
          {expMenu && (
            <div style={{ position: "absolute", bottom: "calc(100% + 6px)", left: 0, zIndex: 30, minWidth: 170,
              background: C.card || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 9,
              boxShadow: "0 14px 36px -10px rgba(0,0,0,.5)", padding: 5, display: "flex", flexDirection: "column", gap: 1 }}>
              {isVec ? EXPORT_FORMATS.map(fmt => (
                <button key={fmt} onClick={() => { setExpMenu(false); fmt === "GeoJSON" ? onExport(l.id) : onExportFmt(l.id, fmt); }}
                  style={{ fontFamily: F, fontSize: 11.5, textAlign: "left", padding: "6px 9px", borderRadius: 6, border: "none", background: "transparent", color: C.txt, cursor: "pointer", width: "100%" }}>{fmt}</button>
              )) : (exportImageTiff && l.kind === "image" && l.imageUrl && l.coordinates)
                ? <button onClick={() => { setExpMenu(false); exportImageTiff(l); }} style={{ fontFamily: F, fontSize: 11.5, textAlign: "left", padding: "6px 9px", borderRadius: 6, border: "none", background: "transparent", color: C.txt, cursor: "pointer", width: "100%" }}>{tiffBusy === l.id ? "Export…" : "GeoTIFF (.tif)"}</button>
                : <div style={{ fontSize: 11, color: C.dim, padding: "6px 9px" }}>Export indisponible</div>}
            </div>
          )}
        </div>
        {footBtn(IcRefresh, "Réinit.", () => onClassify(l.id, null), { disabled: !isVec })}
        <div style={{ flex: 1 }} />
        {footBtn(IcCheck, "OK", () => onClose?.(), { primary: true })}
      </div>
    </div>
  );
}

// ── Fenêtre Symbologie AUTONOME (montée au niveau App) : survit à la fermeture du
//    panneau Couches et participe à l'ordre d'empilement partagé (clic = au 1er plan).
export function SymbologyWindow({ layer, onClose, onStyle, onClassify, onExport, onExportFmt, onRemove, onUpdateRasterLayer, onUpdateGeojson, mapRef, openStats, exportImageTiff, tiffBusy }) {
  const [z, setZ] = useState(() => nextZ());
  if (!layer) return null;
  return (
    <PropsWindow title={layer.name} isRaster={layer.isRaster} badge={geomLabel(layer)}
      z={z} onFocus={() => setZ(v => bumpZ(v))} onClose={onClose}>
      <LayerSymbology l={layer} geomLabel={geomLabel(layer)} onClose={onClose}
        onStyle={onStyle} onClassify={onClassify} onExport={onExport} onExportFmt={onExportFmt}
        onRemove={onRemove} onUpdateRasterLayer={onUpdateRasterLayer} onUpdateGeojson={onUpdateGeojson}
        mapRef={mapRef} openStats={openStats} exportImageTiff={exportImageTiff} tiffBusy={tiffBusy} />
    </PropsWindow>
  );
}

// ── Composant principal ────────────────────────────────────────
export default function LayerPanel({ layers, onToggle, onRemove, onStyle, onExport, onClassify, onExportFmt, onRename, onMoveUp, onMoveDown, onReorder, onZoomExtent, onUpdateRasterLayer, onFilter, mapRef, onUpdateGeojson, onOpenSymbology, openId }) {
  const C = useThemeContext();
  const exp = openId;   // couche dont la fenêtre de symbologie est ouverte (état porté par App)
  const [editName, setEditName] = useState(null);
  const [dragId,   setDragId]   = useState(null);   // glisser-déposer : couche saisie
  const [overId,   setOverId]   = useState(null);   // couche survolée (indicateur de dépôt)
  const [tiffBusy, setTiffBusy] = useState(null);

  // Export d'une couche image (overlay géoréférencé) → GeoTIFF téléchargé.
  const exportImageTiff = async (l) => {
    if (!l.imageUrl || !l.coordinates) return;
    setTiffBusy(l.id);
    try {
      const r = await fetch(`${API}/api/raster/to_geotiff`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ image_b64: l.imageUrl, coordinates: l.coordinates, name: l.name }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      const bin = atob(d.geotiff_b64); const bytes = new Uint8Array(bin.length);
      for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
      const url = URL.createObjectURL(new Blob([bytes], { type: "image/tiff" }));
      const a = document.createElement("a"); a.href = url; a.download = `${(l.name || "couche").replace(/[^\w.-]+/g, "_")}.tif`;
      document.body.appendChild(a); a.click(); a.remove(); setTimeout(() => URL.revokeObjectURL(url), 2000);
    } catch (e) { alert("Export GeoTIFF : " + (e.message || e)); }
    finally { setTiffBusy(null); }
  };

  // ── État modale statistiques (générique, tous rasters GEE) ───
  const [statsModal, setStatsModal] = useState(null);

  const openStats = (e, l) => {
    e.stopPropagation();
    const gp = l._geeParams || {};
    setStatsModal({
      layer:      l,
      dataset:    gp.dataset,
      index:      gp.index,
      bbox:       gp.bbox || l.bbox || null,
      roiGeoJSON: gp.roi_geojson || null,
      geeParams:  gp,
    });
  };

  // ── État modale filtre ───────────────────────────────────────
  const [filterModal, setFilterModal] = useState(null);

  const openFilter = (e, l) => {
    e.stopPropagation();
    setFilterModal(l);
  };

  return (
    <div style={{
      display: "flex", flexDirection: "column",
      width: "100%", height: "100%", minHeight: 0, overflow: "hidden",
    }}>

      {/* En-tête */}
      <div style={{
        padding: "8px 14px", borderBottom: `0.5px solid ${C.bdr}`,
        fontSize: 12, fontWeight: 600, color: C.txt,
        flexShrink: 0, display: "flex", alignItems: "center", gap: 6,
      }}>
        Couches
        {layers.length > 0 && (
          <span style={{
            background: C.acc, color: "#fff", borderRadius: 8,
            fontSize: 10, padding: "0 6px", fontWeight: 700, lineHeight: "16px",
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
          <div key={l.id}
            onDragOver={e => { if (dragId && dragId !== l.id) { e.preventDefault(); if (overId !== l.id) setOverId(l.id); } }}
            onDragLeave={() => setOverId(o => (o === l.id ? null : o))}
            onDrop={e => { e.preventDefault(); if (dragId && dragId !== l.id) onReorder?.(dragId, l.id); setDragId(null); setOverId(null); }}
            style={{ borderBottom: `0.5px solid ${C.bdr}`,
              boxShadow: overId === l.id ? `inset 0 2px 0 ${C.acc}` : "none",
              opacity: dragId === l.id ? 0.45 : 1 }}>

            {/* Ligne principale — double-clic = renommer ; symbologie via le bouton palette */}
            <div
              style={{
                padding: "7px 10px", display: "flex", alignItems: "center", gap: 6,
                cursor: "default", background: exp === l.id ? C.hover : "transparent",
              }}
              onDoubleClick={() => setEditName(l.id)}
              title="Double-clic : renommer"
            >
              <span
                draggable
                onDragStart={e => { setDragId(l.id); e.dataTransfer.effectAllowed = "move"; try { e.dataTransfer.setData("text/plain", l.id); } catch (_) {} }}
                onDragEnd={() => { setDragId(null); setOverId(null); }}
                onClick={e => e.stopPropagation()}
                title="Glisser pour réordonner"
                style={{ cursor: "grab", color: C.dim, lineHeight: 0, padding: "0 1px", flexShrink: 0, userSelect: "none", display: "inline-flex", alignItems: "center" }}
              ><IcMove size={13} /></span>
              <div style={{
                width: 10, height: 10, borderRadius: 3,
                background: l.color, opacity: l.visible ? 1 : 0.3, flexShrink: 0,
              }} />

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
              <button onClick={e => { e.stopPropagation(); onOpenSymbology?.(exp === l.id ? null : l.id); }} title="Symbologie / propriétés"
                style={{ background: exp === l.id ? C.acc + "22" : "none", border:`0.5px solid ${exp === l.id ? C.acc : C.bdr}`, borderRadius:4, cursor:"pointer", padding:"2px 4px", color: exp === l.id ? C.acc : C.dim, lineHeight:0, flexShrink:0, display:"flex", alignItems:"center" }}><IcPalette size={13} /></button>
              <button onClick={e => { e.stopPropagation(); onZoomExtent?.(l.id); }} title="Zoomer"
                style={{ background:"none",border:"none",cursor:"pointer",fontSize:11,padding:"0 2px",color:C.dim,lineHeight:1,flexShrink:0 }}>🔍</button>
              <button onClick={e => { e.stopPropagation(); onMoveUp?.(l.id); }} title="Monter"
                style={{ background:"none",border:"none",cursor:"pointer",fontSize:10,padding:"0 1px",color:C.dim,lineHeight:1,flexShrink:0 }}>▲</button>
              <button onClick={e => { e.stopPropagation(); onMoveDown?.(l.id); }} title="Descendre"
                style={{ background:"none",border:"none",cursor:"pointer",fontSize:10,padding:"0 1px",color:C.dim,lineHeight:1,flexShrink:0 }}>▼</button>
              {l.classResult && <span style={{ fontSize:9, color:C.acc, flexShrink:0 }} title="Classifié">●</span>}

              {/* ── Bouton stats (tous rasters GEE avec dataset/index) ── */}
              {l.isRaster && l._geeParams?.dataset && l._geeParams?.index && (
                <button
                  onClick={e => openStats(e, l)}
                  title="Statistiques de la zone"
                  style={{
                    background: "none", border: `0.5px solid ${C.acc}44`,
                    borderRadius: 4, cursor: "pointer",
                    padding: "3px 5px", color: C.acc, flexShrink: 0, display: "flex", alignItems: "center",
                  }}
                >
                  <IcBarChart size={13}/>
                </button>
              )}

              {/* ── Bouton filtre attributaire (couches vecteur) ── */}
              {!l.isRaster && l.geojson && (
                <button
                  onClick={e => openFilter(e, l)}
                  title={
                    l.filterState?.rules?.length
                      ? `Filtre actif — ${l.filterState.rules.length} règle${l.filterState.rules.length > 1 ? "s" : ""}`
                      : "Filtrer par attribut"
                  }
                  style={{
                    background: l.filterState?.rules?.length ? C.acc + "22" : "none",
                    border: `0.5px solid ${l.filterState?.rules?.length ? C.acc : C.bdr}`,
                    borderRadius: 4, cursor: "pointer",
                    padding: "2px 5px",
                    color: l.filterState?.rules?.length ? C.acc : C.dim,
                    flexShrink: 0,
                    display: "flex", alignItems: "center", gap: 3,
                    lineHeight: 1,
                    transition: "all .15s",
                  }}
                >
                  <FunnelIcon
                    size={11}
                    color={l.filterState?.rules?.length ? C.acc : C.dim}
                    filled={!!l.filterState?.rules?.length}
                  />
                  {l.filterState?.rules?.length > 0 && (
                    <span style={{ fontSize: 9, fontFamily: M, fontWeight: 700 }}>
                      {l.filterState.rules.length}
                    </span>
                  )}
                </button>
              )}

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
              {/* Corbeille — supprimer la couche */}
              <button onClick={e => { e.stopPropagation(); onRemove(l.id); }} title="Supprimer la couche"
                style={{ background: "none", border: `0.5px solid ${C.bdr}`, borderRadius: 4, cursor: "pointer", padding: "2px 4px", color: C.dim, lineHeight: 0, flexShrink: 0, display: "flex", alignItems: "center" }}
                onMouseEnter={e => { e.currentTarget.style.color = C.red; e.currentTarget.style.borderColor = C.red; }}
                onMouseLeave={e => { e.currentTarget.style.color = C.dim; e.currentTarget.style.borderColor = C.bdr; }}>
                <IcTrash size={12} />
              </button>
            </div>

            {/* Mini-légende classification (toujours visible sous la ligne) */}
            {l.isRaster && l.legend?.length > 0 && (
              <div style={{
                display: "flex", flexWrap: "wrap", gap: "2px 8px",
                padding: "0 10px 5px 26px",
              }}>
                {l.legend.slice(0, 8).map(e => (
                  <div key={e.class_id} style={{ display:"flex", alignItems:"center", gap:3 }}>
                    <div style={{
                      width: 7, height: 7, borderRadius: 1, flexShrink: 0,
                      background: e.color, border: `0.5px solid rgba(0,0,0,.15)`,
                    }} />
                    <span style={{ fontSize: 9, color: C.dim }}>
                      {e.label}
                    </span>
                    {fmtArea(e.area_ha) && (
                      <span style={{ fontSize: 8, color: C.mut }}>
                        ({fmtArea(e.area_ha)})
                      </span>
                    )}
                  </div>
                ))}
                {l.legend.length > 8 && (
                  <span style={{ fontSize: 9, color: C.dim }}>
                    +{l.legend.length - 8}
                  </span>
                )}
              </div>
            )}
            {/* La fenêtre Symbologie est désormais montée au niveau App (autonome),
                ouverte via onOpenSymbology → elle survit à la fermeture du panneau. */}
          </div>
        ))}
      </div>

      {/* ── Modale statistiques (générique GEE, bonnes variables dataset/index) ── */}
      {statsModal?.dataset && (
        <IndexStatsModal
          dataset={statsModal.dataset}
          index={statsModal.index}
          layer={statsModal.layer}
          bbox={statsModal.bbox}
          roiGeoJSON={statsModal.roiGeoJSON}
          geeParams={statsModal.geeParams}
          onClose={() => setStatsModal(null)}
        />
      )}

      {/* ── Modale filtre attributaire ── */}
      {filterModal && (
        <FilterModal
          layer={filterModal}
          onClose={() => setFilterModal(null)}
          onApply={(filterState) => {
            if (!onFilter) return;
            // La source originale est préservée dans _sourceGeojson.
            // Si elle n'existe pas encore, on la crée à partir du geojson actuel.
            const sourceGeojson = filterModal._sourceGeojson || filterModal.geojson;

            if (!filterState?.rules?.length) {
              // Effacement du filtre : restaurer la source complète
              onFilter(filterModal.id, {
                filterState: { rules: [], logic: "AND" },
                geojson: sourceGeojson,
                _sourceGeojson: sourceGeojson,
              });
            } else {
              // Appliquer le filtre : calculer le geojson filtré
              const filtered = applyFilter(sourceGeojson, filterState);
              onFilter(filterModal.id, {
                filterState,
                geojson: filtered,
                _sourceGeojson: sourceGeojson,
              });
            }
          }}
        />
      )}    </div>
  );
}
