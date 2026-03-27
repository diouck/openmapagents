/**
 * OGCPanel.jsx — Module WMS / WMTS / WFS / Tuiles vectorielles
 *
 * Architecture 100% frontend :
 *   WMS/WMTS  → source MapLibre type:"raster" (URL template)
 *   WFS       → fetch GeoJSON + reprojection proj4 si CRS ≠ EPSG:4326
 *   VectorTiles → source MapLibre type:"vector"
 *
 * Dépendance : npm install proj4
 */
import { useState, useCallback, useRef } from "react";
import { getTheme } from "../theme";
import { F, M } from "../config";

// ── Presets de services connus ────────────────────────────────
const PRESETS = [
  {
    label: "IGN Géoportail WMS",
    type: "wms",
    url: "https://data.geopf.fr/wms-r/wms",
    version: "1.3.0",
    crs: "EPSG:3857",
  },
  {
    label: "IGN Géoportail WFS",
    type: "wfs",
    url: "https://data.geopf.fr/wfs/ows",
    version: "2.0.0",
    crs: "EPSG:4326",
  },
  {
    label: "IGN WMTS (TileMatrixSet PM)",
    type: "wmts",
    url: "https://data.geopf.fr/wmts",
    version: "1.0.0",
    crs: "EPSG:3857",
  },
  {
    label: "OpenStreetMap WMS",
    type: "wms",
    url: "https://ows.terrestris.de/osm/service",
    version: "1.3.0",
    crs: "EPSG:3857",
  },
];

const SERVICE_TYPES = [
  { key: "wms",    label: "WMS",    icon: "🖼", desc: "Images raster" },
  { key: "wmts",   label: "WMTS",   icon: "⬛", desc: "Tuiles raster" },
  { key: "wfs",    label: "WFS",    icon: "🔷", desc: "Vecteur GeoJSON" },
  { key: "vector", label: "Vecteur", icon: "🗺", desc: "MVT / TileJSON" },
];

const CRS_OPTIONS = [
  { value: "EPSG:4326",  label: "EPSG:4326 — WGS84" },
  { value: "EPSG:3857",  label: "EPSG:3857 — Web Mercator" },
  { value: "EPSG:2154",  label: "EPSG:2154 — Lambert-93" },
  { value: "EPSG:27572", label: "EPSG:27572 — Lambert IIe" },
  { value: "custom",     label: "Autre (saisir)" },
];

// ── Helpers GetCapabilities ───────────────────────────────────
async function fetchCapabilities(url, serviceType, version) {
  const versionParam = version || "1.3.0";
  const params = new URLSearchParams({
    SERVICE: serviceType.toUpperCase(),
    REQUEST: "GetCapabilities",
    VERSION: versionParam,
  });
  const sep = url.includes("?") ? "&" : "?";
  const fullUrl = `${url}${sep}${params}`;
  const res = await fetch(fullUrl);
  if (!res.ok) throw new Error(`GetCapabilities échoué (${res.status})`);
  const text = await res.text();
  return new window.DOMParser().parseFromString(text, "text/xml");
}

function parseWMSLayers(xml) {
  const layers = [];
  const nodes = xml.querySelectorAll("Layer > Layer, Layer[queryable]");
  nodes.forEach(node => {
    const name  = node.querySelector(":scope > Name")?.textContent?.trim();
    const title = node.querySelector(":scope > Title")?.textContent?.trim();
    if (!name) return;

    // Extraire la bbox géographique (WMS 1.3 = EX_GeographicBoundingBox, WMS 1.1 = LatLonBoundingBox)
    let bbox = null;
    const exBbox = node.querySelector(":scope > EX_GeographicBoundingBox");
    if (exBbox) {
      const w = parseFloat(exBbox.querySelector("westBoundLongitude")?.textContent);
      const e = parseFloat(exBbox.querySelector("eastBoundLongitude")?.textContent);
      const s = parseFloat(exBbox.querySelector("southBoundLatitude")?.textContent);
      const n = parseFloat(exBbox.querySelector("northBoundLatitude")?.textContent);
      if (!isNaN(w) && !isNaN(e) && !isNaN(s) && !isNaN(n)) bbox = [w, s, e, n];
    }
    if (!bbox) {
      const llBbox = node.querySelector(":scope > LatLonBoundingBox");
      if (llBbox) {
        const w = parseFloat(llBbox.getAttribute("minx"));
        const s = parseFloat(llBbox.getAttribute("miny"));
        const e = parseFloat(llBbox.getAttribute("maxx"));
        const n = parseFloat(llBbox.getAttribute("maxy"));
        if (!isNaN(w) && !isNaN(e) && !isNaN(s) && !isNaN(n)) bbox = [w, s, e, n];
      }
    }
    layers.push({ name, title: title || name, bbox });
  });
  return layers;
}

function parseWFSTypes(xml) {
  const types = [];
  xml.querySelectorAll("FeatureType").forEach(ft => {
    const name  = ft.querySelector("Name")?.textContent?.trim();
    const title = ft.querySelector("Title")?.textContent?.trim();
    const crs   = ft.querySelector("DefaultCRS, DefaultSRS")?.textContent?.trim();
    if (name) types.push({ name, title: title || name, crs: crs || "EPSG:4326" });
  });
  return types;
}

function parseWMTSLayers(xml) {
  const layers = [];
  xml.querySelectorAll("Layer").forEach(l => {
    const id    = l.querySelector("ows\\:Identifier, Identifier")?.textContent?.trim();
    const title = l.querySelector("ows\\:Title, Title")?.textContent?.trim();
    const tms   = [];
    l.querySelectorAll("TileMatrixSetLink TileMatrixSet").forEach(t => tms.push(t.textContent?.trim()));
    const formats = [];
    l.querySelectorAll("Format").forEach(f => formats.push(f.textContent?.trim()));
    if (id) layers.push({ name: id, title: title || id, tileMatrixSets: tms, formats });
  });
  return layers;
}

// ── Reprojection WFS avec proj4 ───────────────────────────────
async function reprojectGeoJSON(geojson, fromCRS, toCRS = "EPSG:4326") {
  if (fromCRS === toCRS || fromCRS === "EPSG:4326" || fromCRS === "urn:ogc:def:crs:OGC:1.3:CRS84") {
    return geojson;
  }
  try {
    const proj4 = (await import("proj4")).default;

    // Définitions EPSG courantes
    const defs = {
      "EPSG:2154":  "+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 +x_0=700000 +y_0=6600000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs",
      "EPSG:27572": "+proj=lcc +lat_1=46.8 +lat_0=46.8 +lon_0=0 +k_0=0.99987742 +x_0=600000 +y_0=2200000 +a=6378249.2 +b=6356515 +towgs84=-168,-60,320,0,0,0,0 +pm=paris +units=m +no_defs",
      "EPSG:3857":  "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs",
      "EPSG:4326":  "+proj=longlat +datum=WGS84 +no_defs",
    };

    // Enregistrer les projections
    if (defs[fromCRS]) proj4.defs(fromCRS, defs[fromCRS]);
    if (defs[toCRS])   proj4.defs(toCRS,   defs[toCRS]);

    // Reprojeter chaque feature
    const converter = proj4(fromCRS, toCRS);

    const reproject = (coords) => {
      if (!Array.isArray(coords)) return coords;
      if (typeof coords[0] === "number") {
        // Gestion axe inversé EPSG:4326 (lat, lon) vs (lon, lat)
        const [a, b] = coords;
        const c = converter.forward([a, b]);
        return c;
      }
      return coords.map(reproject);
    };

    const features = geojson.features.map(f => ({
      ...f,
      geometry: f.geometry ? {
        ...f.geometry,
        coordinates: reproject(f.geometry.coordinates),
      } : null,
    }));

    return { ...geojson, features };
  } catch (e) {
    console.warn("Reprojection échouée, utilisation des coords brutes :", e.message);
    return geojson;
  }
}

// ── Construit l'URL WMS pour MapLibre ─────────────────────────
function buildWMSUrl(url, layer, version, crs, extraParams = {}) {
  const isCRS84 = crs === "EPSG:4326";
  // WMS 1.3.0 utilise CRS=, WMS 1.1.x utilise SRS=
  const crsKey = version?.startsWith("1.3") ? "CRS" : "SRS";
  const bboxParam = isCRS84 ? "BBOX={bbox-epsg-4326}" : "BBOX={bbox-epsg-3857}";
  const effectiveCRS = isCRS84 ? "EPSG:4326" : "EPSG:3857";

  const params = new URLSearchParams({
    SERVICE:     "WMS",
    VERSION:     version || "1.3.0",
    REQUEST:     "GetMap",
    LAYERS:      layer,
    STYLES:      "",
    FORMAT:      "image/png",
    TRANSPARENT: "TRUE",
    WIDTH:       "256",
    HEIGHT:      "256",
    [crsKey]:    effectiveCRS,
    ...extraParams,
  });
  const sep = url.includes("?") ? "&" : "?";
  return `${url}${sep}${params}&${bboxParam}`;
}

// ── Construit l'URL WMTS pour MapLibre ────────────────────────
function buildWMTSUrl(url, layer, tileMatrixSet, format = "image/png") {
  const sep = url.includes("?") ? "&" : "?";
  return `${url}${sep}SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&LAYER=${layer}&STYLE=normal&TILEMATRIXSET=${tileMatrixSet}&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}&FORMAT=${encodeURIComponent(format)}`;
}

// ── Composant champ ───────────────────────────────────────────
function Field({ label, children }) {
  const C = getTheme();
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
      <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>{label}</div>
      {children}
    </div>
  );
}

// ── Composant principal ───────────────────────────────────────
export default function OGCPanel({ mapRef, onAddRasterLayer, onAddLayer }) {
  const C = getTheme();

  // Config service
  const [serviceType, setServiceType] = useState("wms");
  const [url,         setUrl]         = useState("https://data.geopf.fr/wms-r/wms");
  const [version,     setVersion]     = useState("1.3.0");
  const [crs,         setCrs]         = useState("EPSG:3857");
  const [customCrs,   setCustomCrs]   = useState("");

  // Capabilities
  const [loading,     setLoading]     = useState(false);
  const [capLayers,   setCapLayers]   = useState([]);
  const [capError,    setCapError]    = useState(null);
  const [capLoaded,   setCapLoaded]   = useState(false);

  // Sélection layer
  const [selectedLayer,  setSelectedLayer]  = useState(null);
  const [layerSearch,    setLayerSearch]    = useState("");

  // WFS options
  const [wfsCrs,        setWfsCrs]        = useState("EPSG:4326");
  const [wfsMaxFeatures, setWfsMaxFeatures] = useState(1000);
  const [wfsBbox,        setWfsBbox]       = useState(true);

  // WMTS options
  const [tileMatrixSet, setTileMatrixSet] = useState("PM");
  const [wmtsFormat,    setWmtsFormat]    = useState("image/png");

  // Vector tiles
  const [vtUrl,    setVtUrl]    = useState("");
  const [vtType,   setVtType]   = useState("url"); // "url" | "tilejson"
  const [vtSource, setVtSource] = useState("");    // nom de source dans le style

  // UI
  const [status,   setStatus]   = useState(null);  // {type: ok|error|info, msg}
  const [loadingLayer, setLoadingLayer] = useState(false);
  const [activeTab, setActiveTab] = useState("service"); // "service" | "layers"

  const effectiveCrs = crs === "custom" ? customCrs : crs;

  // ── Appliquer un preset ──────────────────────────────────────
  const applyPreset = (p) => {
    setServiceType(p.type);
    setUrl(p.url);
    setVersion(p.version);
    setCrs(p.crs || "EPSG:3857");
    setCapLayers([]);
    setCapLoaded(false);
    setSelectedLayer(null);
    setStatus(null);
    // Versions par défaut selon type
    if (p.type === "wfs")  setVersion("2.0.0");
    if (p.type === "wmts") setVersion("1.0.0");
  };

  // ── GetCapabilities ─────────────────────────────────────────
  const getCapabilities = useCallback(async () => {
    setLoading(true);
    setCapError(null);
    setCapLayers([]);
    setCapLoaded(false);
    setSelectedLayer(null);
    try {
      const xml = await fetchCapabilities(url, serviceType, version);
      let layers = [];
      if (serviceType === "wms")  layers = parseWMSLayers(xml);
      if (serviceType === "wfs")  layers = parseWFSTypes(xml);
      if (serviceType === "wmts") layers = parseWMTSLayers(xml);

      if (!layers.length) {
        setCapError("Aucune couche trouvée dans les capacités.");
      } else {
        setCapLayers(layers);
        setCapLoaded(true);
        setActiveTab("layers");
        setStatus({ type: "ok", msg: `${layers.length} couche(s) disponible(s)` });
      }
    } catch (e) {
      setCapError(`GetCapabilities échoué : ${e.message}`);
    }
    setLoading(false);
  }, [url, serviceType, version]);

  // ── Ajouter une couche WMS ───────────────────────────────────
  const addWMSLayer = useCallback((layer) => {
    try {
      const tileUrl  = buildWMSUrl(url, layer.name, version, effectiveCrs);
      const sourceId = `wms_${layer.name.replace(/[^a-zA-Z0-9_]/g, "_")}_${Date.now()}`;
      onAddRasterLayer?.({
        id: sourceId, mapSourceId: sourceId,
        mapLayerId: `${sourceId}-layer`,
        name: layer.title || layer.name,
        type: "wms", tileUrl, opacity: 0.85,
        bbox: layer.bbox || null,
      });
      setStatus({ type: "ok", msg: `✓ WMS "${layer.title || layer.name}" ajouté` });
    } catch (e) {
      setStatus({ type: "error", msg: `Erreur WMS : ${e.message}` });
    }
  }, [url, version, effectiveCrs, onAddRasterLayer]);

  // ── Ajouter une couche WMTS ──────────────────────────────────
  const addWMTSLayer = useCallback((layer) => {
    try {
      const tms     = tileMatrixSet || layer.tileMatrixSets?.[0] || "PM";
      const fmt     = wmtsFormat    || layer.formats?.[0]        || "image/png";
      const tileUrl = buildWMTSUrl(url, layer.name, tms, fmt);
      const sourceId = `wmts_${layer.name.replace(/[^a-zA-Z0-9_]/g, "_")}_${Date.now()}`;
      onAddRasterLayer?.({
        id: sourceId, mapSourceId: sourceId,
        mapLayerId: `${sourceId}-layer`,
        name: layer.title || layer.name,
        type: "wmts", tileUrl, opacity: 0.85,
        bbox: layer.bbox || null,
      });
      setStatus({ type: "ok", msg: `✓ WMTS "${layer.title || layer.name}" ajouté` });
    } catch (e) {
      setStatus({ type: "error", msg: `Erreur WMTS : ${e.message}` });
    }
  }, [url, tileMatrixSet, wmtsFormat, onAddRasterLayer]);

  // ── Ajouter une couche WFS ───────────────────────────────────
  const addWFSLayer = useCallback(async (layer) => {
    setLoadingLayer(true);
    setStatus({ type: "info", msg: `Chargement WFS "${layer.title || layer.name}"…` });
    try {
      const srcCrs = layer.crs || wfsCrs || "EPSG:4326";

      // Construire l'URL WFS GetFeature
      const params = new URLSearchParams({
        SERVICE:      "WFS",
        VERSION:      version || "2.0.0",
        REQUEST:      "GetFeature",
        TYPENAMES:    layer.name,
        OUTPUTFORMAT: "application/json",
        COUNT:        String(wfsMaxFeatures),
        SRSNAME:      srcCrs,
      });

      // Bbox optionnelle depuis la vue carte actuelle
      if (wfsBbox) {
        const map = mapRef.current?.getMap?.();
        if (map) {
          const b = map.getBounds();
          const bboxStr = srcCrs === "EPSG:2154"
            ? "" // Skip bbox pour Lambert (conversion complexe)
            : `${b.getWest()},${b.getSouth()},${b.getEast()},${b.getNorth()},EPSG:4326`;
          if (bboxStr) params.set("BBOX", bboxStr);
        }
      }

      const sep = url.includes("?") ? "&" : "?";
      const res = await fetch(`${url}${sep}${params}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      let geojson = await res.json();

      if (!geojson?.features?.length) {
        throw new Error("Aucune feature retournée (bbox trop petite ou couche vide)");
      }

      // Reprojeter si nécessaire
      const needsReproj = srcCrs !== "EPSG:4326" && srcCrs !== "urn:ogc:def:crs:OGC:1.3:CRS84" && srcCrs !== "CRS:84";
      if (needsReproj) {
        setStatus({ type: "info", msg: `Reprojection ${srcCrs} → EPSG:4326…` });
        geojson = await reprojectGeoJSON(geojson, srcCrs, "EPSG:4326");
      }

      const name = layer.title || layer.name;
      onAddLayer(geojson, `WFS — ${name}`, "wfs");
      setStatus({ type: "ok", msg: `✓ WFS "${name}" : ${geojson.features.length} features ajoutées` });
    } catch (e) {
      setStatus({ type: "error", msg: `Erreur WFS : ${e.message}` });
    }
    setLoadingLayer(false);
  }, [url, version, wfsCrs, wfsMaxFeatures, wfsBbox, mapRef, onAddLayer]);

  // ── Ajouter tuiles vectorielles ──────────────────────────────
  const addVectorTiles = useCallback(() => {
    if (!vtUrl.trim()) return setStatus({ type: "error", msg: "URL requise" });
    try {
      const sourceId = `vt_${Date.now()}`;
      onAddRasterLayer?.({
        id: sourceId, mapSourceId: sourceId,
        mapLayerId: `${sourceId}-fill`,
        name: `VT — ${vtUrl.slice(0, 40)}`,
        type: "vector", tileUrl: vtUrl, opacity: 1,
      });
      setStatus({ type: "ok", msg: `✓ Tuiles vectorielles ajoutées` });
    } catch (e) {
      setStatus({ type: "error", msg: `Erreur : ${e.message}` });
    }
  }, [vtUrl, vtType, vtSource, onAddRasterLayer]);

  // ── Filtrer les couches ──────────────────────────────────────
  const filteredLayers = capLayers.filter(l =>
    !layerSearch ||
    l.name.toLowerCase().includes(layerSearch.toLowerCase()) ||
    l.title?.toLowerCase().includes(layerSearch.toLowerCase())
  );

  const inp = {
    fontFamily: F, fontSize: 11, padding: "5px 8px",
    borderRadius: 5, background: C.input, color: C.txt,
    border: `0.5px solid ${C.bdr}`, outline: "none",
    width: "100%", boxSizing: "border-box",
  };

  const statusColors = { ok: C.acc, error: C.red, info: C.amb };

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" }}>

      {/* ── Onglets ──────────────────────────────────────────── */}
      <div style={{ display: "flex", borderBottom: `0.5px solid ${C.bdr}`, flexShrink: 0 }}>
        {[["service", "⚙ Service"], ["layers", `📋 Couches${capLayers.length ? ` (${capLayers.length})` : ""}`]].map(([key, label]) => (
          <button key={key} onClick={() => setActiveTab(key)} style={{
            fontFamily: F, fontSize: 11, padding: "8px 0", border: "none", cursor: "pointer", flex: 1,
            background: activeTab === key ? C.acc + "15" : "transparent",
            color: activeTab === key ? C.acc : C.mut,
            borderBottom: activeTab === key ? `2px solid ${C.acc}` : "2px solid transparent",
          }}>{label}</button>
        ))}
      </div>

      <div style={{ flex: 1, overflowY: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 10 }}>

        {/* ══ ONGLET SERVICE ════════════════════════════════════ */}
        {activeTab === "service" && (
          <>
            {/* Presets */}
            <div>
              <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 4 }}>Services prédéfinis</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                {PRESETS.map(p => (
                  <button key={p.label} onClick={() => applyPreset(p)} style={{
                    fontFamily: F, fontSize: 10, padding: "5px 10px", borderRadius: 5, textAlign: "left",
                    background: url === p.url && serviceType === p.type ? C.acc + "18" : "transparent",
                    border: `0.5px solid ${url === p.url && serviceType === p.type ? C.acc + "55" : C.bdr}`,
                    color: url === p.url && serviceType === p.type ? C.acc : C.mut, cursor: "pointer",
                    display: "flex", alignItems: "center", gap: 8,
                  }}>
                    <span>{SERVICE_TYPES.find(s => s.key === p.type)?.icon}</span>
                    <span>{p.label}</span>
                    <span style={{ marginLeft: "auto", fontSize: 9, color: C.dim }}>{p.type.toUpperCase()}</span>
                  </button>
                ))}
              </div>
            </div>

            <div style={{ borderTop: `0.5px solid ${C.bdr}` }} />

            {/* Type de service */}
            <Field label="Type de service">
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 4 }}>
                {SERVICE_TYPES.map(s => (
                  <button key={s.key} onClick={() => {
                    setServiceType(s.key);
                    setCapLayers([]); setCapLoaded(false); setSelectedLayer(null);
                    setVersion(s.key === "wmts" ? "1.0.0" : s.key === "wfs" ? "2.0.0" : "1.3.0");
                  }} style={{
                    fontFamily: F, fontSize: 10, padding: "6px 4px", borderRadius: 5, cursor: "pointer",
                    background: serviceType === s.key ? C.acc + "18" : "transparent",
                    border: `0.5px solid ${serviceType === s.key ? C.acc + "66" : C.bdr}`,
                    color: serviceType === s.key ? C.acc : C.dim,
                    display: "flex", flexDirection: "column", alignItems: "center", gap: 2,
                  }}>
                    <span style={{ fontSize: 14 }}>{s.icon}</span>
                    <span style={{ fontSize: 9 }}>{s.label}</span>
                  </button>
                ))}
              </div>
            </Field>

            {/* URL + Version */}
            <Field label="URL du service">
              <input value={url} onChange={e => setUrl(e.target.value)}
                placeholder="https://..." style={inp} />
            </Field>

            {serviceType !== "vector" && (
              <div style={{ display: "flex", gap: 6 }}>
                <div style={{ flex: 1 }}>
                  <Field label="Version">
                    <select value={version} onChange={e => setVersion(e.target.value)} style={{ ...inp }}>
                      {serviceType === "wms"  && <><option value="1.3.0">1.3.0</option><option value="1.1.1">1.1.1</option><option value="1.1.0">1.1.0</option></>}
                      {serviceType === "wfs"  && <><option value="2.0.0">2.0.0</option><option value="1.1.0">1.1.0</option><option value="1.0.0">1.0.0</option></>}
                      {serviceType === "wmts" && <option value="1.0.0">1.0.0</option>}
                    </select>
                  </Field>
                </div>
                <div style={{ flex: 2 }}>
                  <Field label="CRS demandé">
                    <select value={crs} onChange={e => setCrs(e.target.value)} style={{ ...inp }}>
                      {CRS_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                    </select>
                  </Field>
                </div>
              </div>
            )}

            {crs === "custom" && (
              <Field label="CRS personnalisé (ex: EPSG:27572)">
                <input value={customCrs} onChange={e => setCustomCrs(e.target.value)}
                  placeholder="EPSG:XXXXX" style={inp} />
              </Field>
            )}

            {/* Options spécifiques WFS */}
            {serviceType === "wfs" && (
              <div style={{ background: C.hover, borderRadius: 6, padding: 8, border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 6 }}>
                <div style={{ fontSize: 9, color: C.acc, textTransform: "uppercase", letterSpacing: ".05em" }}>Options WFS</div>
                <div style={{ display: "flex", gap: 6 }}>
                  <div style={{ flex: 1 }}>
                    <Field label="Max features">
                      <input type="number" value={wfsMaxFeatures} onChange={e => setWfsMaxFeatures(parseInt(e.target.value))} style={inp} />
                    </Field>
                  </div>
                  <div style={{ flex: 2 }}>
                    <Field label="CRS source (pour reprojection)">
                      <select value={wfsCrs} onChange={e => setWfsCrs(e.target.value)} style={{ ...inp }}>
                        {CRS_OPTIONS.filter(o => o.value !== "custom").map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                      </select>
                    </Field>
                  </div>
                </div>
                <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, color: C.mut, cursor: "pointer" }}>
                  <input type="checkbox" checked={wfsBbox} onChange={e => setWfsBbox(e.target.checked)} />
                  Filtrer par emprise carte (BBOX)
                </label>
                <div style={{ fontSize: 9, color: C.dim, lineHeight: 1.5 }}>
                  La reprojection vers EPSG:4326 est automatique.<br/>
                  Requis : <code style={{ fontFamily: M }}>npm install proj4</code>
                </div>
              </div>
            )}

            {/* Options spécifiques WMTS */}
            {serviceType === "wmts" && (
              <div style={{ background: C.hover, borderRadius: 6, padding: 8, border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 6 }}>
                <div style={{ fontSize: 9, color: C.acc, textTransform: "uppercase", letterSpacing: ".05em" }}>Options WMTS</div>
                <div style={{ display: "flex", gap: 6 }}>
                  <div style={{ flex: 1 }}>
                    <Field label="TileMatrixSet">
                      <select value={tileMatrixSet} onChange={e => setTileMatrixSet(e.target.value)} style={{ ...inp }}>
                        <option value="PM">PM (Web Mercator)</option>
                        <option value="WGS84">WGS84</option>
                        <option value="LAMB93">LAMB93</option>
                        <option value="custom">Autre</option>
                      </select>
                    </Field>
                  </div>
                  <div style={{ flex: 1 }}>
                    <Field label="Format">
                      <select value={wmtsFormat} onChange={e => setWmtsFormat(e.target.value)} style={{ ...inp }}>
                        <option value="image/png">image/png</option>
                        <option value="image/jpeg">image/jpeg</option>
                        <option value="image/webp">image/webp</option>
                      </select>
                    </Field>
                  </div>
                </div>
              </div>
            )}

            {/* Tuiles vectorielles */}
            {serviceType === "vector" && (
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                <div style={{ display: "flex", gap: 4 }}>
                  {[["url", "URL template {z}/{x}/{y}"], ["tilejson", "TileJSON URL"]].map(([k, l]) => (
                    <button key={k} onClick={() => setVtType(k)} style={{
                      fontFamily: F, fontSize: 10, padding: "4px 0", borderRadius: 4, flex: 1,
                      background: vtType === k ? C.acc + "18" : "transparent",
                      border: `0.5px solid ${vtType === k ? C.acc + "55" : C.bdr}`,
                      color: vtType === k ? C.acc : C.dim, cursor: "pointer",
                    }}>{l}</button>
                  ))}
                </div>
                <Field label={vtType === "tilejson" ? "URL TileJSON" : "URL template tuiles"}>
                  <input value={vtUrl} onChange={e => setVtUrl(e.target.value)}
                    placeholder={vtType === "tilejson" ? "https://.../tiles.json" : "https://.../{z}/{x}/{y}.pbf"}
                    style={inp} />
                </Field>
                <Field label="Source layer (optionnel)">
                  <input value={vtSource} onChange={e => setVtSource(e.target.value)}
                    placeholder="nom_de_la_source_layer"
                    style={inp} />
                </Field>
                <button onClick={addVectorTiles} disabled={!vtUrl.trim()} style={{
                  fontFamily: F, fontSize: 11, fontWeight: 500, padding: "8px 12px", borderRadius: 6,
                  background: vtUrl.trim() ? C.acc : C.hover, color: vtUrl.trim() ? "#fff" : C.dim,
                  border: "none", cursor: vtUrl.trim() ? "pointer" : "default",
                }}>➕ Ajouter tuiles vectorielles</button>
              </div>
            )}

            {/* Statut */}
            {status && (
              <div style={{ fontSize: 10, padding: "5px 8px", borderRadius: 5, background: statusColors[status.type] + "15", border: `0.5px solid ${statusColors[status.type]}44`, color: statusColors[status.type], lineHeight: 1.5 }}>
                {status.msg}
              </div>
            )}

            {capError && (
              <div style={{ fontSize: 10, color: C.red, background: C.red + "12", border: `0.5px solid ${C.red}33`, borderRadius: 5, padding: "6px 8px", lineHeight: 1.5 }}>
                ❌ {capError}
              </div>
            )}

            {/* Bouton GetCapabilities */}
            {serviceType !== "vector" && (
              <button onClick={getCapabilities} disabled={loading || !url.trim()} style={{
                fontFamily: F, fontSize: 11, fontWeight: 500, padding: "8px 12px", borderRadius: 6,
                background: url.trim() ? C.acc : C.hover, color: url.trim() ? "#fff" : C.dim,
                border: "none", cursor: url.trim() ? "pointer" : "default",
                opacity: loading ? 0.6 : 1,
              }}>
                {loading ? "⏳ Chargement capacités…" : "📡 GetCapabilities"}
              </button>
            )}
          </>
        )}

        {/* ══ ONGLET COUCHES ════════════════════════════════════ */}
        {activeTab === "layers" && (
          <>
            {!capLoaded && serviceType !== "vector" ? (
              <div style={{ textAlign: "center", padding: "30px 0", color: C.dim, fontSize: 12 }}>
                <div style={{ fontSize: 28, marginBottom: 8 }}>📡</div>
                Cliquez "GetCapabilities" dans l'onglet Service
                <br />
                <button onClick={() => setActiveTab("service")} style={{ marginTop: 10, fontFamily: F, fontSize: 10, padding: "4px 10px", borderRadius: 4, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.acc, cursor: "pointer" }}>
                  ← Aller au service
                </button>
              </div>
            ) : (
              <>
                {/* Recherche */}
                <input value={layerSearch} onChange={e => setLayerSearch(e.target.value)}
                  placeholder="Rechercher une couche…"
                  style={{ ...inp, fontSize: 11 }} />

                {/* Statut/erreur */}
                {status && (
                  <div style={{ fontSize: 10, padding: "4px 8px", borderRadius: 4, background: statusColors[status.type] + "15", color: statusColors[status.type] }}>
                    {status.msg}
                  </div>
                )}

                {/* Options WFS rappel */}
                {serviceType === "wfs" && (
                  <div style={{ fontSize: 9, color: C.dim, background: C.hover, borderRadius: 4, padding: "4px 8px", display: "flex", alignItems: "center", gap: 6 }}>
                    <span>Max: {wfsMaxFeatures} ft</span>
                    <span>·</span>
                    <span>CRS source: {wfsCrs}</span>
                    <span>·</span>
                    <span>{wfsBbox ? "BBOX active" : "sans BBOX"}</span>
                    <button onClick={() => setActiveTab("service")} style={{ marginLeft: "auto", fontFamily: F, fontSize: 9, background: "none", border: "none", color: C.acc, cursor: "pointer" }}>✎</button>
                  </div>
                )}

                {/* WMTS options rappel */}
                {serviceType === "wmts" && (
                  <div style={{ display: "flex", gap: 6 }}>
                    <div style={{ flex: 1 }}>
                      <Field label="TileMatrixSet">
                        <select value={tileMatrixSet} onChange={e => setTileMatrixSet(e.target.value)} style={{ ...inp, fontSize: 10 }}>
                          <option value="PM">PM</option>
                          <option value="WGS84">WGS84</option>
                          <option value="LAMB93">LAMB93</option>
                        </select>
                      </Field>
                    </div>
                    <div style={{ flex: 1 }}>
                      <Field label="Format">
                        <select value={wmtsFormat} onChange={e => setWmtsFormat(e.target.value)} style={{ ...inp, fontSize: 10 }}>
                          <option value="image/png">PNG</option>
                          <option value="image/jpeg">JPEG</option>
                        </select>
                      </Field>
                    </div>
                  </div>
                )}

                {/* Liste des couches */}
                <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                  {filteredLayers.length === 0 && (
                    <div style={{ fontSize: 11, color: C.dim, textAlign: "center", padding: "20px 0" }}>
                      Aucune couche correspondante
                    </div>
                  )}
                  {filteredLayers.map(layer => (
                    <div key={layer.name}
                      style={{
                        padding: "7px 10px", borderRadius: 6, border: `0.5px solid ${selectedLayer?.name === layer.name ? C.acc + "66" : C.bdr}`,
                        background: selectedLayer?.name === layer.name ? C.acc + "12" : C.hover,
                        cursor: "pointer", display: "flex", flexDirection: "column", gap: 2,
                      }}
                      onClick={() => setSelectedLayer(layer)}
                    >
                      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <span style={{ fontSize: 12 }}>{SERVICE_TYPES.find(s => s.key === serviceType)?.icon}</span>
                        <span style={{ fontSize: 11, color: C.txt, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {layer.title || layer.name}
                        </span>
                        {layer.crs && layer.crs !== "EPSG:4326" && (
                          <span style={{ fontSize: 9, color: C.amb, background: C.amb + "15", padding: "1px 5px", borderRadius: 3 }}>
                            {layer.crs.split(":").pop()}
                          </span>
                        )}
                      </div>
                      {layer.title && layer.title !== layer.name && (
                        <div style={{ fontSize: 9, color: C.dim, paddingLeft: 20, fontFamily: M }}>
                          {layer.name}
                        </div>
                      )}
                      {layer.tileMatrixSets?.length > 0 && (
                        <div style={{ fontSize: 9, color: C.dim, paddingLeft: 20 }}>
                          TMS: {layer.tileMatrixSets.join(", ")}
                        </div>
                      )}
                    </div>
                  ))}
                </div>

                {/* Bouton ajouter */}
                {selectedLayer && (
                  <button
                    onClick={() => {
                      if (serviceType === "wms")  addWMSLayer(selectedLayer);
                      if (serviceType === "wmts") addWMTSLayer(selectedLayer);
                      if (serviceType === "wfs")  addWFSLayer(selectedLayer);
                    }}
                    disabled={loadingLayer}
                    style={{
                      fontFamily: F, fontSize: 11, fontWeight: 500, padding: "9px 14px", borderRadius: 6,
                      background: C.acc, color: "#fff", border: "none",
                      cursor: loadingLayer ? "default" : "pointer",
                      opacity: loadingLayer ? 0.6 : 1,
                      position: "sticky", bottom: 0,
                    }}
                  >
                    {loadingLayer
                      ? "⏳ Chargement…"
                      : `➕ Ajouter "${(selectedLayer.title || selectedLayer.name).slice(0, 25)}"`}
                  </button>
                )}
              </>
            )}
          </>
        )}
      </div>
    </div>
  );
}
