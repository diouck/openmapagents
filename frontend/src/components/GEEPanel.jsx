/**
 * GEEPanel.jsx — Module Google Earth Engine
 * Datasets : Sentinel-2, Landsat 8/9, MODIS LST/NDVI,
 *            ESA WorldCover, Sentinel-1 SAR, Hansen Forest, ERA5, SRTM
 */
import { useState, useEffect, useCallback, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";

// ── Couleurs par groupe de dataset ───────────────────────────
const DS_COLORS = {
  "Optique": "#1D9E75",  "Radar": "#5B8DD9",
  "Climat":  "#EF9F27",  "Végétation": "#6BBF5E",
  "Forêt":   "#2E7D32",  "Relief": "#8D6E63",
};

const DATASET_GROUPS = {
  "Optique": [
    { id: "sentinel2", label: "Sentinel-2",   desc: "10m · 5 jours",  icon: "🛰️" },
    { id: "landsat9",  label: "Landsat 9",    desc: "30m · 16 jours", icon: "🛰️" },
    { id: "landsat8",  label: "Landsat 8",    desc: "30m · 16 jours", icon: "🛰️" },
  ],
  "Radar": [
    { id: "sentinel1", label: "Sentinel-1 SAR", desc: "10m · tout temps", icon: "📡" },
  ],
  "Végétation": [
    { id: "modis_ndvi",  label: "MODIS NDVI",    desc: "500m · 16 jours", icon: "🌿" },
    { id: "worldcover",  label: "ESA WorldCover", desc: "10m · annuel",    icon: "🗺️" },
    { id: "hansen",      label: "Forest Watch",   desc: "30m · annuel",    icon: "🌳" },
  ],
  "Climat": [
    { id: "modis_lst", label: "MODIS LST Temp.", desc: "1km · quotidien", icon: "🌡️" },
    { id: "era5",      label: "ERA5 Climat",     desc: "11km · mensuel",  icon: "🌦️" },
  ],
  "Relief": [
    { id: "srtm", label: "SRTM Relief", desc: "30m · statique", icon: "⛰️" },
  ],
};

const INDICES = {
  sentinel2:  ["RGB", "NDVI", "NDWI", "NDBI", "EVI", "False Color (NIR)"],
  landsat9:   ["RGB", "NDVI", "NDWI", "LST (température)"],
  landsat8:   ["RGB", "NDVI", "NDWI", "LST (température)"],
  sentinel1:  ["VV", "VH", "VV/VH"],
  modis_ndvi: ["NDVI", "EVI"],
  modis_lst:  ["LST Jour", "LST Nuit"],
  worldcover: ["Occupation du sol"],
  hansen:     ["Couverture forêt 2000", "Perte forêt", "Gain forêt"],
  era5:       ["Température air", "Précipitations", "Humidité"],
  srtm:       ["Élévation", "Pente", "Ombrage"],
};

const NEEDS_DATES = new Set(["sentinel2","landsat9","landsat8","sentinel1","modis_ndvi","modis_lst","era5"]);
const NEEDS_CLOUD = new Set(["sentinel2","landsat9","landsat8"]);
const STATIC_DS   = new Set(["srtm","hansen","worldcover"]); // ee.Image — pas de dates

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ── Composant calendrier compact ──────────────────────────────
function DatePicker({ dates, value, onChange }) {
  const C = useThemeContext();
  const months = [...new Set(dates.map(d => d.slice(0, 7)))];
  const [selMonth, setSelMonth] = useState(months[months.length - 1] || "");
  const daysInMonth = dates.filter(d => d.startsWith(selMonth));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
      {/* Sélecteur de mois */}
      <select value={selMonth} onChange={e => setSelMonth(e.target.value)}
        style={{ fontFamily: M, fontSize: 10, padding: "4px 6px", borderRadius: 5, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none" }}>
        {months.map(m => <option key={m} value={m}>{m}</option>)}
      </select>

      {/* Grille des jours disponibles */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 3 }}>
        {daysInMonth.map(d => {
          const day = d.slice(8);
          const sel = d === value;
          return (
            <button key={d} onClick={() => onChange(d)} title={d}
              style={{
                fontFamily: M, fontSize: 10, width: 28, height: 22, borderRadius: 4,
                background: sel ? C.acc : C.hover,
                color:      sel ? "#fff" : C.mut,
                border:     sel ? `1.5px solid ${C.acc}` : `0.5px solid ${C.bdr}`,
                cursor: "pointer", fontWeight: sel ? 600 : 400,
              }}>{day}</button>
          );
        })}
        {!daysInMonth.length && (
          <span style={{ fontSize: 10, color: C.dim }}>Aucune image ce mois</span>
        )}
      </div>
    </div>
  );
}

// ── Panel principal ───────────────────────────────────────────
export default function GEEPanel({ mapRef, onAddRasterLayer, layers = [], sidebarWidth = 0, chatWidth = 0 }) {
  const C = useThemeContext();

  const [dataset,   setDataset]   = useState("sentinel2");
  const [index,     setIndex]     = useState("NDVI");
  const [dateStart, setDateStart] = useState("2025-03-29");
  const [dateEnd,   setDateEnd]   = useState("2026-03-29");
  const [selDate,   setSelDate]   = useState("");
  const [composite, setComposite] = useState("least_cloudy");
  const [cloudMax,  setCloudMax]  = useState(20);
  const [opacity,   setOpacity]   = useState(0.85);
  const [openGroup, setOpenGroup] = useState("Optique");
  const [roiMode,    setRoiMode]   = useState("bbox"); // "bbox" | "layer"
  const [roiLayerId, setRoiLayerId] = useState("");

  const [dates,     setDates]     = useState([]);
  const [loadDates, setLoadDates] = useState(false);
  const [loading,   setLoading]   = useState(false);
  const [status,    setStatus]    = useState(null); // {type, msg}
  const [geeReady,  setGeeReady]  = useState(null); // null=checking, true, false

  const needsDates = NEEDS_DATES.has(dataset);
  const needsCloud = NEEDS_CLOUD.has(dataset);
  const indices    = INDICES[dataset] || [];

  // Check GEE health au montage
  useEffect(() => {
    fetch(`${API}/api/gee/health`)
      .then(r => r.json())
      .then(d => setGeeReady(d.ready))
      .catch(() => setGeeReady(false));
  }, []);

  // Reset index quand on change de dataset
  const handleDataset = (id) => {
    setDataset(id);
    setIndex(INDICES[id]?.[0] || "");
    setDates([]); setSelDate(""); setStatus(null);
  };

  // Récupère bbox ou géométrie ROI selon le mode
  const getRoi = useCallback(() => {
    if (roiMode === "layer" && roiLayerId) {
      const layer = layers.find(l => l.id === roiLayerId);
      if (layer?.geojson?.features?.length) {
        const polys = layer.geojson.features.filter(
          f => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
        );
        if (polys.length > 0) {
          const geom = polys.length === 1
            ? polys[0].geometry
            : { type: "GeometryCollection", geometries: polys.map(f => f.geometry) };
          return { roi_geojson: geom, bbox: null };
        }
        // Fallback bbox de la couche
        const coords = layer.geojson.features.flatMap(f => {
          const g = f.geometry; if (!g) return [];
          if (g.type === "Point") return [g.coordinates];
          if (g.type === "LineString") return g.coordinates;
          if (g.type === "Polygon") return g.coordinates[0];
          return [];
        });
        if (coords.length) {
          const lons = coords.map(c => c[0]), lats = coords.map(c => c[1]);
          return { roi_geojson: null, bbox: [Math.min(...lons), Math.min(...lats), Math.max(...lons), Math.max(...lats)] };
        }
      }
    }
    // Mode bbox carte (défaut) — compensée par sidebar + chat
    try {
      const map = mapRef.current?.getMap?.();
      if (map) {
        const canvas = map.getCanvas();
        const W = canvas.offsetWidth;
        const H = canvas.offsetHeight;
        // Décaler pour exclure le sidebar gauche et le panel chat droit
        const left   = sidebarWidth;
        const right  = chatWidth;
        const top    = 0;
        const bottom = 0;
        // Convertir les coins pixel en coordonnées géo
        const sw = map.unproject([left,        H - bottom]);
        const ne = map.unproject([W - right,   top]);
        return { roi_geojson: null, bbox: [sw.lng, sw.lat, ne.lng, ne.lat] };
      }
    } catch(_) {}
    return { roi_geojson: null, bbox: null };
  }, [roiMode, roiLayerId, layers, mapRef, sidebarWidth, chatWidth]);

  // Charger les dates disponibles
  const fetchDates = useCallback(async () => {
    setLoadDates(true); setStatus(null);
    try {
      const { bbox, roi_geojson } = getRoi();
      const res = await fetch(`${API}/api/gee/dates`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dataset, bbox, roi_geojson, date_start: dateStart, date_end: dateEnd, cloud_max: cloudMax }),
      });
      // Reset la date sélectionnée quand on recharge les dates
      setSelDate("");
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || "Erreur serveur");
      setDates(data.dates || []);
      setSelDate(data.dates?.[data.dates.length - 1] || "");
      setStatus({ type: "ok", msg: `${data.count} images disponibles` });
    } catch (e) {
      setStatus({ type: "error", msg: e.message });
    }
    setLoadDates(false);
  }, [dataset, dateStart, dateEnd, cloudMax, getRoi]);

  // Charger les tuiles
  const fetchTiles = useCallback(async () => {
    setLoading(true); setStatus(null);
    try {
      // Date unique sélectionnée → end = start + 1 jour (GEE exige une plage)
      let tileStart = dateStart, tileEnd = dateEnd;
      if (selDate) {
        tileStart = selDate;
        const d = new Date(selDate); d.setDate(d.getDate() + 1);
        tileEnd = d.toISOString().slice(0, 10);
      }
      const useDate = selDate || null;
      // Zone d'analyse : bbox carte ou ROI couche
      const { bbox, roi_geojson } = getRoi();

      const res = await fetch(`${API}/api/gee/tiles`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset, index,
          date_start: tileStart,
          date_end:   tileEnd,
          cloud_max:  cloudMax,
          composite,
          bbox,
          roi_geojson,
        }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);

      // Ajouter dans LayerPanel comme couche raster
      const sourceId = `gee_${dataset}_${index.replace(/\s/g,"_")}_${Date.now()}`;
      // Paramètres GEE stockés pour permettre le restyle depuis LayerPanel
      const geeParams = {
        dataset, index,
        date_start: tileStart, date_end: tileEnd,
        cloud_max: cloudMax, composite, bbox, roi_geojson,
      };
      onAddRasterLayer?.({
        id:          sourceId,
        mapSourceId: sourceId,
        mapLayerId:  `${sourceId}-layer`,
        name:        data.name || `${dataset} — ${index}`,
        type:        "wms",
        tileUrl:     data.tile_url,
        opacity,
        bbox:        data.clip_bbox || null,
        visParams:   data.vis_params || null,
        geeParams,
      });

      setStatus({ type: "ok", msg: `✓ Couche ajoutée : ${data.date || "composite"}` });
    } catch (e) {
      setStatus({ type: "error", msg: `Erreur : ${e.message}` });
    }
    setLoading(false);
  }, [dataset, index, selDate, dateStart, dateEnd, cloudMax, composite, opacity, onAddRasterLayer, getRoi]);

  const inp = {
    fontFamily: M, fontSize: 10, padding: "4px 7px", borderRadius: 5,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };

  const statColor = { ok: C.acc, error: C.red, info: C.amb };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" }}>

      {/* ── Status GEE ─────────────────────────────────────── */}
      <div style={{ padding: "5px 12px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", gap: 6, flexShrink: 0 }}>
        <div style={{ width: 7, height: 7, borderRadius: "50%", background: geeReady === true ? C.acc : geeReady === false ? C.red : C.amb, flexShrink: 0 }} />
        <span style={{ fontSize: 10, color: C.dim }}>
          {geeReady === true ? "GEE connecté" : geeReady === false ? "GEE non disponible — vérifiez le backend" : "Connexion GEE…"}
        </span>
      </div>

      <div style={{ flex: 1, overflowY: "auto", padding: 10, display: "flex", flexDirection: "column", gap: 10 }}>

        {/* ── Sélection dataset ─────────────────────────────── */}
        <div>
          <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 4 }}>Dataset</div>
          {Object.entries(DATASET_GROUPS).map(([group, datasets]) => (
            <div key={group} style={{ marginBottom: 2 }}>
              {/* En-tête groupe */}
              <div onClick={() => setOpenGroup(openGroup === group ? "" : group)}
                style={{
                  display: "flex", alignItems: "center", gap: 5, padding: "3px 6px",
                  cursor: "pointer", borderRadius: 4,
                  background: datasets.some(d => d.id === dataset) ? DS_COLORS[group] + "15" : "transparent",
                }}>
                <span style={{ fontSize: 9, fontWeight: 600, color: DS_COLORS[group] || C.dim, textTransform: "uppercase", letterSpacing: ".06em", flex: 1 }}>{group}</span>
                <span style={{ fontSize: 9, color: C.dim }}>{openGroup === group ? "▾" : "▸"}</span>
              </div>
              {/* Datasets du groupe */}
              {openGroup === group && datasets.map(ds => (
                <div key={ds.id} onClick={() => handleDataset(ds.id)}
                  style={{
                    display: "flex", alignItems: "center", gap: 8, padding: "5px 6px 5px 14px",
                    cursor: "pointer", borderRadius: 5, marginTop: 1,
                    background: dataset === ds.id ? (DS_COLORS[group] || C.acc) + "18" : "transparent",
                    border: `0.5px solid ${dataset === ds.id ? (DS_COLORS[group] || C.acc) + "55" : "transparent"}`,
                  }}>
                  <span style={{ fontSize: 14 }}>{ds.icon}</span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontSize: 11, color: dataset === ds.id ? (DS_COLORS[group] || C.acc) : C.txt, fontWeight: dataset === ds.id ? 500 : 400 }}>{ds.label}</div>
                    <div style={{ fontSize: 9, color: C.dim }}>{ds.desc}</div>
                  </div>
                  {dataset === ds.id && <span style={{ fontSize: 10, color: DS_COLORS[group] || C.acc }}>✓</span>}
                </div>
              ))}
            </div>
          ))}
        </div>

        {/* ── Indice / produit ──────────────────────────────── */}
        <div>
          <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 4 }}>Produit / Indice</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 3 }}>
            {indices.map(idx => (
              <button key={idx} onClick={() => setIndex(idx)} style={{
                fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4,
                background: index === idx ? C.acc + "18" : "transparent",
                border: `0.5px solid ${index === idx ? C.acc + "66" : C.bdr}`,
                color: index === idx ? C.acc : C.dim, cursor: "pointer",
                fontWeight: index === idx ? 600 : 400,
              }}>{idx}</button>
            ))}
          </div>
        </div>

        {/* ── Période + filtre nuages ───────────────────────── */}
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
                <span style={{ fontSize: 9, color: C.dim, flexShrink: 0 }}>☁ Max nuages</span>
                <input type="range" min="0" max="100" step="5" value={cloudMax}
                  onChange={e => setCloudMax(parseInt(e.target.value))}
                  style={{ flex: 1, height: 3 }} />
                <span style={{ fontFamily: M, fontSize: 10, color: C.txt, flexShrink: 0 }}>{cloudMax}%</span>
              </div>
            )}

            {/* Bouton charger les dates — masqué pour datasets statiques */}
            {!STATIC_DS.has(dataset) && (
              <button onClick={fetchDates} disabled={loadDates} style={{
                fontFamily: F, fontSize: 10, padding: "5px 10px", borderRadius: 5,
                background: "transparent", border: `0.5px solid ${C.acc}`,
                color: C.acc, cursor: "pointer", opacity: loadDates ? 0.6 : 1,
              }}>
                {loadDates ? "⏳ Chargement…" : "📅 Voir les dates disponibles"}
              </button>
            )}
            {STATIC_DS.has(dataset) && (
              <div style={{ fontSize: 10, color: C.acc, padding: "4px 8px", background: C.acc+"12", borderRadius: 5, border: `0.5px solid ${C.acc}33` }}>
                ✓ Dataset statique — pas de sélection de date
              </div>
            )}

            {/* Calendrier des dates */}
            {dates.length > 0 && (
              <div style={{ background: C.hover, borderRadius: 6, padding: 8, border: `0.5px solid ${C.bdr}` }}>
                <div style={{ fontSize: 9, color: C.dim, marginBottom: 4 }}>
                  {dates.length} images — sélectionner une date
                </div>
                <DatePicker dates={dates} value={selDate} onChange={setSelDate} />
                {selDate && (
                  <div style={{ fontSize: 9, color: C.acc, marginTop: 4 }}>📅 {selDate} sélectionné</div>
                )}
              </div>
            )}
          </div>
        )}

        {/* ── Composite (quand pas de date sélectionnée) ───── */}
        {needsDates && !selDate && (
          <div>
            <div style={{ fontSize: 9, color: C.dim, marginBottom: 4 }}>Composite sur la période</div>
            <div style={{ display: "flex", gap: 3 }}>
              {[["least_cloudy","Moins nuageux"],["median","Médiane"],["mosaic","Mosaïque"]].map(([k, l]) => (
                <button key={k} onClick={() => setComposite(k)} style={{
                  fontFamily: F, fontSize: 9, padding: "3px 7px", borderRadius: 4, flex: 1,
                  background: composite === k ? C.acc + "18" : "transparent",
                  border: `0.5px solid ${composite === k ? C.acc + "55" : C.bdr}`,
                  color: composite === k ? C.acc : C.dim, cursor: "pointer",
                }}>{l}</button>
              ))}
            </div>
          </div>
        )}

        {/* ── Zone d'analyse ───────────────────────────────── */}
        <div style={{ background: C.hover, borderRadius: 6, padding: "8px 10px", border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 6 }}>
          <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Zone d'analyse</div>

          {/* Toggle bbox / couche */}
          <div style={{ display: "flex", gap: 4 }}>
            <button onClick={() => setRoiMode("bbox")} style={{
              fontFamily: F, flex: 1, fontSize: 10, padding: "4px 0", borderRadius: 5, cursor: "pointer",
              background: roiMode === "bbox" ? C.acc + "18" : "transparent",
              border: `0.5px solid ${roiMode === "bbox" ? C.acc + "55" : C.bdr}`,
              color: roiMode === "bbox" ? C.acc : C.dim,
            }}>🗺 Vue carte</button>
            <button onClick={() => setRoiMode("layer")} style={{
              fontFamily: F, flex: 1, fontSize: 10, padding: "4px 0", borderRadius: 5, cursor: "pointer",
              background: roiMode === "layer" ? C.acc + "18" : "transparent",
              border: `0.5px solid ${roiMode === "layer" ? C.acc + "55" : C.bdr}`,
              color: roiMode === "layer" ? C.acc : C.dim,
            }}>⬡ Couche (mask)</button>
          </div>

          {/* Mode bbox */}
          {roiMode === "bbox" && (
            <div style={{ fontSize: 9, color: C.dim }}>
              Zoomez sur la zone souhaitée avant de charger pour trouver les images.
            </div>
          )}

          {/* Mode couche ROI */}
          {roiMode === "layer" && (() => {
            const polyLayers = layers.filter(l =>
              !l.isRaster && l.geojson?.features?.some(
                f => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
              )
            );
            return polyLayers.length === 0 ? (
              <div style={{ fontSize: 9, color: C.amb, padding: "4px 6px", background: C.amb + "12", borderRadius: 4 }}>
                Aucune couche polygone disponible. Chargez une couche de type polygone (ville, région…) d'abord.
              </div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <select value={roiLayerId} onChange={e => setRoiLayerId(e.target.value)}
                  style={{ fontFamily: F, fontSize: 10, padding: "5px 8px", borderRadius: 5, background: C.input, color: C.txt, border: `0.5px solid ${roiLayerId ? C.acc + "66" : C.bdr}`, outline: "none" }}>
                  <option value="">— Choisir une couche —</option>
                  {polyLayers.map(l => (
                    <option key={l.id} value={l.id}>{l.name} ({l.featureCount} entités)</option>
                  ))}
                </select>
                {roiLayerId && (() => {
                  const l = layers.find(x => x.id === roiLayerId);
                  const hasPolygon = l?.geojson?.features?.some(f => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon");
                  return (
                    <div style={{ fontSize: 9, color: C.acc, padding: "4px 6px", background: C.acc + "12", borderRadius: 4 }}>
                      {hasPolygon
                        ? "✓ Masque exact — les rasters suivront le contour de la couche"
                        : "⚠ Pas de polygone — bbox de la couche utilisée"}
                    </div>
                  );
                })()}
              </div>
            );
          })()}
        </div>

        {/* ── Opacité ──────────────────────────────────────── */}
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 9, color: C.dim, flexShrink: 0 }}>Opacité</span>
          <input type="range" min="0.1" max="1" step="0.05" value={opacity}
            onChange={e => setOpacity(parseFloat(e.target.value))}
            style={{ flex: 1, height: 3 }} />
          <span style={{ fontFamily: M, fontSize: 10, color: C.txt, flexShrink: 0 }}>{Math.round(opacity * 100)}%</span>
        </div>

        {/* ── Statut ───────────────────────────────────────── */}
        {status && (
          <div style={{
            fontSize: 10, padding: "5px 8px", borderRadius: 5, lineHeight: 1.5,
            background: statColor[status.type] + "15",
            border: `0.5px solid ${statColor[status.type]}44`,
            color: statColor[status.type],
          }}>{status.msg}</div>
        )}

        {/* ── Bouton charger ───────────────────────────────── */}
        <div style={{ marginTop: "auto" }}>
          <button onClick={fetchTiles} disabled={loading || !geeReady} style={{
            fontFamily: F, fontSize: 12, fontWeight: 600, padding: "9px 0",
            borderRadius: 6, width: "100%",
            background: geeReady && !loading ? C.acc : C.hover,
            color:  geeReady && !loading ? "#fff" : C.dim,
            border: "none", cursor: geeReady && !loading ? "pointer" : "default",
            opacity: loading ? 0.6 : 1,
          }}>
            {loading ? "⏳ Calcul GEE en cours…" : `🛰 Charger ${index || "la couche"}`}
          </button>
          {!geeReady && (
            <div style={{ fontSize: 9, color: C.dim, textAlign: "center", marginTop: 4 }}>
              Démarrez le backend FastAPI avec <code style={{ fontFamily: M }}>gee_routes.py</code>
            </div>
          )}
        </div>

      </div>
    </div>
  );
}
