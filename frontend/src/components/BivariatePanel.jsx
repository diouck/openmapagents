/**
 * BivariatePanel.jsx — Carte de sémiologie bivariée (Google Earth Engine)
 *
 * Croise deux variables raster (ex. Température de surface × NDVI) en une
 * matrice 3×3 : chaque variable est classée en tertiles (Faible/Moyen/Élevé)
 * puis combinée en un code 0..8 visualisé avec une palette bivariée.
 *
 * Rendu à l'intérieur de GEEPanel via le bascule « Simple / Bivarié ».
 */
import { useState, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcCheck, IcCloud, IcMap, IcHexagon, IcGrid } from "../icons";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ── Variables disponibles (mappées vers le backend) ───────────────────────
const BIVAR_VARS = [
  { id: "lst_ls",   dataset: "landsat",       index: "LST (température)", label: "Temp. surface (Landsat auto)", group: "Température", needsDate: true,  needsCloud: true },
  { id: "lst_mod",  dataset: "modis_lst",     index: "LST Jour",         label: "Temp. surface (MODIS)",   group: "Température",    needsDate: true },
  { id: "tair",     dataset: "era5",          index: "Température air",   label: "Temp. air (ERA5)",        group: "Température",    needsDate: true },
  { id: "ndvi_s2",  dataset: "sentinel2",     index: "NDVI",             label: "NDVI (Sentinel-2)",       group: "Végétation",     needsDate: true,  needsCloud: true },
  { id: "ndvi_ls",  dataset: "landsat",       index: "NDVI",             label: "NDVI (Landsat auto)",     group: "Végétation",     needsDate: true,  needsCloud: true },
  { id: "ndvi_mod", dataset: "modis_ndvi",    index: "NDVI",             label: "NDVI (MODIS)",            group: "Végétation",     needsDate: true },
  { id: "evi_s2",   dataset: "sentinel2",     index: "EVI",              label: "EVI (Sentinel-2)",        group: "Végétation",     needsDate: true,  needsCloud: true },
  { id: "canopy",   dataset: "canopy_height", index: "Hauteur canopée",  label: "Hauteur canopée",         group: "Végétation",     needsDate: false },
  { id: "ndwi_s2",  dataset: "sentinel2",     index: "NDWI",             label: "NDWI humidité (S2)",      group: "Eau / Humidité", needsDate: true,  needsCloud: true },
  { id: "precip",   dataset: "era5",          index: "Précipitations",   label: "Précipitations (ERA5)",   group: "Eau / Humidité", needsDate: true },
  { id: "ndbi_s2",  dataset: "sentinel2",     index: "NDBI",             label: "NDBI bâti (S2)",          group: "Urbain",         needsDate: true,  needsCloud: true },
  { id: "elev",     dataset: "srtm",          index: "Élévation",        label: "Élévation (SRTM)",        group: "Relief",         needsDate: false },
  { id: "slope",    dataset: "srtm",          index: "Pente",            label: "Pente (SRTM)",            group: "Relief",         needsDate: false },
];

const VAR_GROUPS = [...new Set(BIVAR_VARS.map(v => v.group))];

// ── Palettes bivariées 3×3 (doivent matcher le backend) ───────────────────
export const BIVAR_PALETTES = {
  temp_ndvi: {
    label: "Temp × Végétation",
    colors: ["#fffccc", "#c2e699", "#1a9850", "#fdae61", "#b9c46a", "#4d9970", "#d7191c", "#a8674a", "#6e6e3b"],
  },
  violet_bleu: {
    label: "Violet / Bleu",
    colors: ["#e8e8e8", "#ace4e4", "#5ac8c8", "#dfb0d6", "#a5add3", "#5698b9", "#be64ac", "#8c62aa", "#3b4994"],
  },
  rouge_bleu: {
    label: "Rouge / Bleu",
    colors: ["#e8e8e8", "#b5c0da", "#6c83b5", "#e4acac", "#ad9ea5", "#5b6c9e", "#c85a5a", "#985356", "#574249"],
  },
  vert_magenta: {
    label: "Vert / Magenta",
    colors: ["#e8e8e8", "#e4acac", "#c85a5a", "#b0d5c0", "#ad9ea5", "#985356", "#5ac8a0", "#739f8e", "#574249"],
  },
};

// ── Mini aperçu de matrice 3×3 ─────────────────────────────────────────────
function PaletteSwatch({ colors, size = 8 }) {
  // colors[code] avec code = ligne*3 + col ; on affiche ligne 0 en bas (Élevé en haut)
  return (
    <div style={{ display: "grid", gridTemplateColumns: `repeat(3, ${size}px)`, gridTemplateRows: `repeat(3, ${size}px)`, gap: 1 }}>
      {[2, 1, 0].map(a =>
        [0, 1, 2].map(b => (
          <div key={`${a}-${b}`} style={{ width: size, height: size, background: colors[a * 3 + b] }} />
        ))
      )}
    </div>
  );
}

export default function BivariatePanel({ mapRef, onAddRasterLayer, layers = [], geeReady, sidebarWidth = 0, chatWidth = 0 }) {
  const C = useThemeContext();

  const [varAId, setVarAId] = useState("lst_ls");
  const [varBId, setVarBId] = useState("ndvi_s2");
  const [palette, setPalette] = useState("temp_ndvi");
  const [dateStart, setDateStart] = useState("2024-06-01");
  const [dateEnd, setDateEnd] = useState("2024-09-30");
  const [cloudMax, setCloudMax] = useState(30);
  const [composite, setComposite] = useState("median");
  const [opacity, setOpacity] = useState(0.85);
  const [roiMode, setRoiMode] = useState("bbox");
  const [roiLayerId, setRoiLayerId] = useState("");
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState(null);

  const varA = BIVAR_VARS.find(v => v.id === varAId);
  const varB = BIVAR_VARS.find(v => v.id === varBId);
  const needsDates = (varA?.needsDate || varB?.needsDate);
  const needsCloud = (varA?.needsCloud || varB?.needsCloud);

  // ── Zone d'analyse : bbox carte ou couche polygone ──────────────────────
  const getRoi = useCallback(() => {
    if (roiMode === "layer" && roiLayerId) {
      const layer = layers.find(l => l.id === roiLayerId);
      const polys = layer?.geojson?.features?.filter(
        f => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
      ) || [];
      if (polys.length > 0) {
        const geom = polys.length === 1
          ? polys[0].geometry
          : { type: "GeometryCollection", geometries: polys.map(f => f.geometry) };
        return { roi_geojson: geom, bbox: null };
      }
    }
    try {
      const map = mapRef.current?.getMap?.();
      if (map) {
        const canvas = map.getCanvas();
        const W = canvas.offsetWidth, H = canvas.offsetHeight;
        const sw = map.unproject([sidebarWidth, H]);
        const ne = map.unproject([W - chatWidth, 0]);
        return { roi_geojson: null, bbox: [sw.lng, sw.lat, ne.lng, ne.lat] };
      }
    } catch (_) {}
    return { roi_geojson: null, bbox: null };
  }, [roiMode, roiLayerId, layers, mapRef, sidebarWidth, chatWidth]);

  const load = useCallback(async () => {
    if (varAId === varBId) {
      setStatus({ type: "error", msg: "Choisissez deux variables différentes." });
      return;
    }
    setLoading(true); setStatus(null);
    try {
      const { bbox, roi_geojson } = getRoi();
      const res = await fetch(`${API}/api/gee/bivariate/tiles`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          var_a: { dataset: varA.dataset, index: varA.index, label: varA.label },
          var_b: { dataset: varB.dataset, index: varB.index, label: varB.label },
          date_start: dateStart, date_end: dateEnd,
          cloud_max: cloudMax, composite,
          bbox, roi_geojson, palette,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);

      const sourceId = `bivar_${varAId}_${varBId}_${Date.now()}`;
      onAddRasterLayer?.({
        id:          sourceId,
        mapSourceId: sourceId,
        mapLayerId:  `${sourceId}-layer`,
        name:        data.name || `Bivarié — ${varA.label} × ${varB.label}`,
        type:        "wms",
        tileUrl:     data.tile_url,
        opacity,
        bbox:        data.clip_bbox || null,
        bivariate:   data.bivariate || null,
      });
      setStatus({ type: "ok", msg: "Carte bivariée ajoutée" });
    } catch (e) {
      setStatus({ type: "error", msg: `Erreur : ${e.message}` });
    }
    setLoading(false);
  }, [varAId, varBId, varA, varB, dateStart, dateEnd, cloudMax, composite, palette, opacity, getRoi, onAddRasterLayer]);

  const inp = {
    fontFamily: M, fontSize: 10, padding: "5px 7px", borderRadius: 5,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };
  const statColor = { ok: C.acc, error: C.red, info: C.amb };

  // Sélecteur de variable — fonction de rendu (pas un composant interne, pour
  // éviter le remontage du sous-arbre à chaque rendu du panneau).
  const renderVarSelect = (value, onChange, badge, badgeColor) => (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
        <span style={{ fontSize: 10, fontWeight: 700, color: "#fff", background: badgeColor, width: 16, height: 16, borderRadius: 4, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>{badge}</span>
        <span style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Variable {badge}</span>
      </div>
      <select value={value} onChange={e => onChange(e.target.value)} style={{ ...inp, border: `0.5px solid ${badgeColor}55` }}>
        {VAR_GROUPS.map(g => (
          <optgroup key={g} label={g}>
            {BIVAR_VARS.filter(v => v.group === g).map(v => (
              <option key={v.id} value={v.id}>{v.label}</option>
            ))}
          </optgroup>
        ))}
      </select>
    </div>
  );

  const polyLayers = layers.filter(l =>
    !l.isRaster && l.geojson?.features?.some(
      f => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
    )
  );

  return (
    <div style={{ flex: 1, overflowY: "auto", padding: 10, display: "flex", flexDirection: "column", gap: 12 }}>

      <div style={{ fontSize: 10, color: C.dim, lineHeight: 1.5, background: C.hover, padding: "7px 9px", borderRadius: 6, border: `0.5px solid ${C.bdr}` }}>
        Croise <b style={{ color: C.txt }}>deux variables</b> en une matrice 3×3.
        Chaque variable est découpée en 3 classes (Faible · Moyen · Élevé) sur la zone analysée.
      </div>

      {/* ── Variables A × B ─────────────────────────────────── */}
      {renderVarSelect(varAId, setVarAId, "A", "#d7391c")}
      <div style={{ textAlign: "center", fontSize: 14, color: C.dim, margin: "-6px 0" }}>×</div>
      {renderVarSelect(varBId, setVarBId, "B", "#1a9850")}
      {varAId === varBId && (
        <div style={{ fontSize: 9, color: C.red }}>Choisissez deux variables différentes.</div>
      )}

      {/* ── Palette ─────────────────────────────────────────── */}
      <div>
        <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 5 }}>Palette bivariée</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          {Object.entries(BIVAR_PALETTES).map(([key, p]) => (
            <button key={key} onClick={() => setPalette(key)} style={{
              display: "flex", alignItems: "center", gap: 8, padding: "5px 7px", borderRadius: 5,
              cursor: "pointer", textAlign: "left", fontFamily: F,
              background: palette === key ? C.acc + "18" : "transparent",
              border: `0.5px solid ${palette === key ? C.acc + "66" : C.bdr}`,
            }}>
              <PaletteSwatch colors={p.colors} />
              <span style={{ fontSize: 10, color: palette === key ? C.acc : C.txt, flex: 1 }}>{p.label}</span>
              {palette === key && <IcCheck size={12} color={C.acc}/>}
            </button>
          ))}
        </div>
      </div>

      {/* ── Période + nuages ────────────────────────────────── */}
      {needsDates && (
        <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
          <div style={{ display: "flex", gap: 6 }}>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 9, color: C.dim, marginBottom: 2 }}>Début</div>
              <input type="date" value={dateStart} onChange={e => setDateStart(e.target.value)} style={inp} />
            </div>
            <div style={{ flex: 1 }}>
              <div style={{ fontSize: 9, color: C.dim, marginBottom: 2 }}>Fin</div>
              <input type="date" value={dateEnd} onChange={e => setDateEnd(e.target.value)} style={inp} />
            </div>
          </div>
          {needsCloud && (
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ fontSize: 9, color: C.dim, flexShrink: 0, display: "inline-flex", alignItems: "center", gap: 4 }}><IcCloud size={11}/> Max nuages</span>
              <input type="range" min="0" max="100" step="5" value={cloudMax}
                onChange={e => setCloudMax(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
              <span style={{ fontFamily: M, fontSize: 10, color: C.txt }}>{cloudMax}%</span>
            </div>
          )}
          <div>
            <div style={{ fontSize: 9, color: C.dim, marginBottom: 4 }}>Composite sur la période</div>
            <div style={{ display: "flex", gap: 3 }}>
              {[["median", "Médiane"], ["least_cloudy", "Moins nuageux"], ["mosaic", "Mosaïque"]].map(([k, l]) => (
                <button key={k} onClick={() => setComposite(k)} style={{
                  fontFamily: F, fontSize: 9, padding: "3px 7px", borderRadius: 4, flex: 1,
                  background: composite === k ? C.acc + "18" : "transparent",
                  border: `0.5px solid ${composite === k ? C.acc + "55" : C.bdr}`,
                  color: composite === k ? C.acc : C.dim, cursor: "pointer",
                }}>{l}</button>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* ── Zone d'analyse ──────────────────────────────────── */}
      <div style={{ background: C.hover, borderRadius: 6, padding: "8px 10px", border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 6 }}>
        <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Zone d'analyse</div>
        <div style={{ display: "flex", gap: 4 }}>
          <button onClick={() => setRoiMode("bbox")} style={{
            fontFamily: F, flex: 1, fontSize: 10, padding: "4px 0", borderRadius: 5, cursor: "pointer",
            background: roiMode === "bbox" ? C.acc + "18" : "transparent",
            border: `0.5px solid ${roiMode === "bbox" ? C.acc + "55" : C.bdr}`,
            color: roiMode === "bbox" ? C.acc : C.dim,
            display: "flex", alignItems: "center", justifyContent: "center", gap: 5,
          }}><IcMap size={12}/> Vue carte</button>
          <button onClick={() => setRoiMode("layer")} style={{
            fontFamily: F, flex: 1, fontSize: 10, padding: "4px 0", borderRadius: 5, cursor: "pointer",
            background: roiMode === "layer" ? C.acc + "18" : "transparent",
            border: `0.5px solid ${roiMode === "layer" ? C.acc + "55" : C.bdr}`,
            color: roiMode === "layer" ? C.acc : C.dim,
            display: "flex", alignItems: "center", justifyContent: "center", gap: 5,
          }}><IcHexagon size={12}/> Couche (mask)</button>
        </div>
        {roiMode === "bbox" && (
          <div style={{ fontSize: 9, color: C.dim }}>Zoomez sur la zone souhaitée avant de charger.</div>
        )}
        {roiMode === "layer" && (
          polyLayers.length === 0 ? (
            <div style={{ fontSize: 9, color: C.amb, padding: "4px 6px", background: C.amb + "12", borderRadius: 4 }}>
              Aucune couche polygone disponible.
            </div>
          ) : (
            <select value={roiLayerId} onChange={e => setRoiLayerId(e.target.value)}
              style={{ ...inp, border: `0.5px solid ${roiLayerId ? C.acc + "66" : C.bdr}` }}>
              <option value="">— Choisir une couche —</option>
              {polyLayers.map(l => <option key={l.id} value={l.id}>{l.name}</option>)}
            </select>
          )
        )}
      </div>

      {/* ── Opacité ─────────────────────────────────────────── */}
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span style={{ fontSize: 9, color: C.dim, flexShrink: 0 }}>Opacité</span>
        <input type="range" min="0.1" max="1" step="0.05" value={opacity}
          onChange={e => setOpacity(parseFloat(e.target.value))} style={{ flex: 1, height: 3 }} />
        <span style={{ fontFamily: M, fontSize: 10, color: C.txt }}>{Math.round(opacity * 100)}%</span>
      </div>

      {/* ── Statut ──────────────────────────────────────────── */}
      {status && (
        <div style={{
          fontSize: 10, padding: "5px 8px", borderRadius: 5, lineHeight: 1.5,
          background: statColor[status.type] + "15",
          border: `0.5px solid ${statColor[status.type]}44`,
          color: statColor[status.type],
        }}>{status.msg}</div>
      )}

      {/* ── Bouton charger ──────────────────────────────────── */}
      <div style={{ marginTop: "auto" }}>
        <button onClick={load} disabled={loading || !geeReady || varAId === varBId} style={{
          fontFamily: F, fontSize: 12, fontWeight: 600, padding: "9px 0", borderRadius: 6, width: "100%",
          background: geeReady && !loading && varAId !== varBId ? C.acc : C.hover,
          color: geeReady && !loading && varAId !== varBId ? "#fff" : C.dim,
          border: "none", cursor: geeReady && !loading && varAId !== varBId ? "pointer" : "default",
          opacity: loading ? 0.6 : 1,
        }}>
          <span style={{ display: "inline-flex", alignItems: "center", gap: 7 }}><IcGrid size={14}/> {loading ? "Calcul GEE en cours…" : "Générer la carte bivariée"}</span>
        </button>
      </div>
    </div>
  );
}
