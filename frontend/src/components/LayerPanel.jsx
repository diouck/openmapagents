import { useState, useEffect } from "react";
import { useThemeContext } from "../theme";
import { F, M, EXPORT_FORMATS } from "../config";
import { Badge, Btn } from "./ui";
import { IcBarChart, IcEye, IcEyeOff } from "../icons";
import ClassPanel from "./ClassPanel";
import FieldCalcBlock from "./FieldCalcBlock";
import IndexStatsModal from "./IndexStatsModal";
import FilterModal, { FunnelIcon, applyFilter } from "./FilterModal";
import { getPC, applyPCStyle, ASPRS_CLASSES } from "../utils/lidarStyle";

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

// ── Composant principal ────────────────────────────────────────
export default function LayerPanel({ layers, onToggle, onRemove, onStyle, onExport, onClassify, onExportFmt, onRename, onMoveUp, onMoveDown, onZoomExtent, onUpdateRasterLayer, onFilter, mapRef, onUpdateGeojson }) {
  const C = useThemeContext();
  const [exp,      setExp]      = useState(null);
  const [editName, setEditName] = useState(null);
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
          <div key={l.id} style={{ borderBottom: `0.5px solid ${C.bdr}` }}>

            {/* Ligne principale */}
            <div
              style={{
                padding: "7px 10px", display: "flex", alignItems: "center", gap: 6,
                cursor: "pointer", background: exp === l.id ? C.hover : "transparent",
              }}
              onClick={() => setExp(exp === l.id ? null : l.id)}
            >
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

            {/* Panneau déroulé */}
            {exp === l.id && (
              <div style={{ padding: "8px 12px 12px", display: "flex", flexDirection: "column", gap: 8, background: C.hover }}>

                <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 11 }}>
                  <span style={{ color: C.dim }}>Opacité</span>
                  <input type="range" min="0" max="1" step="0.05" value={l.opacity}
                    onChange={e => onStyle(l.id, { opacity: parseFloat(e.target.value) })} style={{ flex: 1, height: 3 }} />
                  <span style={{ color: C.dim, fontFamily: M, flexShrink: 0 }}>{Math.round(l.opacity * 100)}%</span>
                </div>

                {l.isRaster && l.visParams && (
                  <RasterStylePanel layer={l} onUpdateLayer={(id, updates) => onUpdateRasterLayer?.(id, updates)} />
                )}

                {/* ── Sémiologie d'un nuage LiDAR (couleur, filtre, légende classes) ── */}
                {l.kind === "pointcloud" && (
                  <PointcloudStylePanel layer={l} mapRef={mapRef} />
                )}

                {/* ── Reclassification d'un GeoTIFF importé (mono OU multi-bande) ── */}
                {l.kind === "image" && l.rasterToken && (
                  <RasterImageStylePanel layer={l} onUpdate={(id, updates) => onUpdateRasterLayer?.(id, updates)} />
                )}

                {/* ── Légende + restyle pour couches de classification ── */}
                {l.isRaster && l.legend?.length > 0 && (
                  <ClassifLegendPanel layer={l} onUpdateRasterLayer={onUpdateRasterLayer} C={C} />
                )}

                {/* ── Bouton stats dans le panneau déroulé (tous rasters GEE) ── */}
                {l.isRaster && l._geeParams?.dataset && l._geeParams?.index && (
                  <button onClick={e => openStats(e, l)} style={{
                    fontFamily: F, fontSize: 10, padding: "6px 0", borderRadius: 5, width: "100%",
                    background: "transparent", border: `0.5px solid ${C.acc}`,
                    color: C.acc, cursor: "pointer", fontWeight: 500,
                    display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
                  }}>
                    <IcBarChart size={13}/> Statistiques de la zone
                  </button>
                )}

                {/* ── Export GeoTIFF d'une couche image géoréférencée ── */}
                {l.kind === "image" && l.imageUrl && l.coordinates && (
                  <button onClick={() => exportImageTiff(l)} disabled={tiffBusy === l.id} style={{
                    fontFamily: F, fontSize: 10, fontWeight: 500, padding: "6px 0", borderRadius: 5, width: "100%",
                    background: "transparent", border: `0.5px solid ${C.acc}`, color: C.acc,
                    cursor: tiffBusy === l.id ? "wait" : "pointer",
                    display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
                  }}>{tiffBusy === l.id ? "Export…" : "⬇ Exporter en GeoTIFF"}</button>
                )}

                {l.isRaster && (
                  <button onClick={() => onRemove(l.id)} style={{
                    fontFamily: F, fontSize: 10, padding: "5px 0", borderRadius: 5, width: "100%",
                    background: "transparent", border: `0.5px solid ${C.red}55`,
                    color: C.red, cursor: "pointer",
                  }}>Supprimer la couche</button>
                )}

                {!l.isRaster && l.geojson && (<>
                <div style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 11 }}>
                  <span style={{ color: C.dim }}>Couleur</span>
                  <input type="color" value={l.color} onChange={e => onStyle(l.id, { color: e.target.value })}
                    style={{ width: 24, height: 18, border: "none", borderRadius: 3, cursor: "pointer", background: "none" }} />
                  <span style={{ color: C.dim }}>Taille</span>
                  <input type="range" min="2" max="15" step="1" value={l.radius || 5}
                    onChange={e => onStyle(l.id, { radius: parseInt(e.target.value) })} style={{ flex: 1, height: 3 }} />
                  <span style={{ color: C.dim, fontFamily: M }}>{l.radius || 5}px</span>
                </div>

                {/* Calculateur de champ : en amont de la classification, puisqu'il
                    sert justement à fabriquer la variable qu'on va classer. */}
                <FieldCalcBlock layer={l} onApply={(gj, col) => onUpdateGeojson?.(l.id, gj, col)} />

                <ClassPanel key={`${l.id}-${l.classCfg?.ramp}-${l.classCfg?.type}`} layer={l} classification={l.classCfg}
                  onChange={cfg => onClassify(l.id, cfg)} mapRef={mapRef}
                  chartCfg={l.chartCfg} onChartChange={cfg => onStyle(l.id, { chartCfg: cfg })}
                  onLayerOpacity={o => onStyle(l.id, { opacity: o })} />

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
