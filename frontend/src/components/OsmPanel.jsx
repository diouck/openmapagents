/**
 * OsmPanel.jsx — Import OSM haute performance
 *
 * Propriétés nettoyées : nom, adresse, catégorie, sous-catégorie, hauteur, téléphone, horaires
 * Catégories POI hiérarchiques avec groupes sélectionnables (checkbox groupe + items)
 * Couleurs par catégorie affichées sur la carte via expression MapLibre
 */
import React, { useState, useCallback, useEffect, useRef } from "react";
import maplibregl from "maplibre-gl";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcUtensils, IcCart, IcBag, IcHospital, IcLandmark, IcHotel,
  IcCircleDot, IcSquare, IcHexagon, IcMapPin, IcCrosshair, IcAlert, IcCheck } from "../icons";

// ─── Hiérarchie POI ───────────────────────────────────────────
const POI_GROUPS = [
  {
    key: "food", label: "Restaurants & Bars", color: "#e53e3e", icon: IcUtensils,
    items: [
      { key: "restaurant",  label: "Restaurants", q: '"amenity"="restaurant"' },
      { key: "cafe",        label: "Cafés",        q: '"amenity"="cafe"' },
      { key: "fast_food",   label: "Fast-food",    q: '"amenity"="fast_food"' },
      { key: "bar",         label: "Bars",         q: '"amenity"="bar"' },
      { key: "pub",         label: "Pubs",         q: '"amenity"="pub"' },
    ],
  },
  {
    key: "grocery", label: "Commerces alimentaires", color: "#38a169", icon: IcCart,
    items: [
      { key: "supermarket", label: "Supermarchés", q: '"shop"="supermarket"' },
      { key: "convenience", label: "Épiceries",    q: '"shop"="convenience"' },
      { key: "bakery",      label: "Boulangeries", q: '"shop"="bakery"' },
      { key: "butcher",     label: "Boucheries",   q: '"shop"="butcher"' },
    ],
  },
  {
    key: "shopping", label: "Boutiques & Shopping", color: "#d53f8c", icon: IcBag,
    items: [
      { key: "clothes",     label: "Vêtements",    q: '"shop"="clothes"' },
      { key: "shoes",       label: "Chaussures",   q: '"shop"="shoes"' },
      { key: "jewelry",     label: "Bijouteries",  q: '"shop"="jewelry"' },
      { key: "electronics", label: "Électronique", q: '"shop"="electronics"' },
      { key: "florist",     label: "Fleuristes",   q: '"shop"="florist"' },
    ],
  },
  {
    key: "health", label: "Santé & Bien-être", color: "#3182ce", icon: IcHospital,
    items: [
      { key: "pharmacy",    label: "Pharmacies",           q: '"amenity"="pharmacy"' },
      { key: "doctor",      label: "Médecins",             q: '"amenity"="doctors"' },
      { key: "dentist",     label: "Dentistes",            q: '"amenity"="dentist"' },
      { key: "optician",    label: "Opticiens",            q: '"shop"="optician"' },
      { key: "hairdresser", label: "Coiffeurs",            q: '"shop"="hairdresser"' },
      { key: "beauty",      label: "Instituts de beauté",  q: '"shop"="beauty"' },
    ],
  },
  {
    key: "services", label: "Services", color: "#744210", icon: IcLandmark,
    items: [
      { key: "bank",       label: "Banques",          q: '"amenity"="bank"' },
      { key: "atm",        label: "Distributeurs",    q: '"amenity"="atm"' },
      { key: "post_office",label: "Bureaux de poste", q: '"amenity"="post_office"' },
      { key: "car_repair", label: "Garages auto",     q: '"shop"="car_repair"' },
      { key: "fuel",       label: "Stations-service", q: '"amenity"="fuel"' },
    ],
  },
  {
    key: "lodging", label: "Hébergement", color: "#6b46c1", icon: IcHotel,
    items: [
      { key: "hotel",       label: "Hôtels",           q: '"tourism"="hotel"' },
      { key: "guest_house", label: "Chambres d'hôtes", q: '"tourism"="guest_house"' },
      { key: "hostel",      label: "Auberges",         q: '"tourism"="hostel"' },
    ],
  },
];

const LINE_PRESETS = [
  { key: "motorway",   label: "Autoroutes",         q: '"highway"~"motorway|motorway_link"',   color: "#e53e3e" },
  { key: "primary",    label: "Routes principales", q: '"highway"~"primary|secondary"',        color: "#dd6b20" },
  { key: "tertiary",   label: "Routes tertiaires",  q: '"highway"~"tertiary|residential"',     color: "#b7791f" },
  { key: "cycleway",   label: "Pistes cyclables",   q: '"highway"="cycleway"',                 color: "#38a169" },
  { key: "footway",    label: "Chemins piétons",    q: '"highway"~"footway|path|pedestrian"',  color: "#3182ce" },
  { key: "railway",    label: "Voies ferrées",      q: '"railway"="rail"',                     color: "#1a202c" },
  { key: "river",      label: "Rivières",           q: '"waterway"~"river|stream|canal"',      color: "#2b6cb0" },
  { key: "power_line", label: "Lignes électriques", q: '"power"="line"',                       color: "#975a16" },
];

const POLYGON_PRESETS = [
  { key: "building",    label: "Bâtiments",            q: '"building"',                          color: "#718096" },
  { key: "forest",      label: "Forêts",               q: '"landuse"~"forest|wood"',             color: "#276749" },
  { key: "residential", label: "Zones résidentielles", q: '"landuse"="residential"',             color: "#e2e8f0" },
  { key: "industrial",  label: "Zones industrielles",  q: '"landuse"="industrial"',              color: "#b7791f" },
  { key: "farmland",    label: "Terres agricoles",     q: '"landuse"~"farmland|meadow|grass"',   color: "#f6e05e" },
  { key: "water",       label: "Plans d'eau",          q: '"natural"="water"',                   color: "#3182ce" },
  { key: "park",        label: "Parcs",                q: '"leisure"="park"',                    color: "#38a169" },
  { key: "cemetery",    label: "Cimetières",           q: '"landuse"="cemetery"',                color: "#4a5568" },
  { key: "retail",      label: "Zones commerciales",   q: '"landuse"="retail"',                  color: "#fc8181" },
];

const TYPE_LABELS = { poi: "Points d'intérêt", line: "Lignes", polygon: "Polygones" };
const TYPE_COLORS = { poi: "#f59e0b", line: "#378ADD", polygon: "#1D9E75" };

const RADIUS_PRESETS = [
  { label: "Vue",   value: 0    },
  { label: "250 m", value: 250  },
  { label: "500 m", value: 500  },
  { label: "1 km",  value: 1000 },
  { label: "2 km",  value: 2000 },
  { label: "5 km",  value: 5000 },
];

const MIRRORS = [
  "https://overpass-api.de/api/interpreter",
  "https://overpass.kumi.systems/api/interpreter",
];

// ─── Nettoyage propriétés OSM ─────────────────────────────────
// Garde uniquement les champs utiles à la carto : nom, adresse, catégorie,
// sous-catégorie, hauteur, téléphone, horaires
function cleanProps(tags, osmId, osmType, catKey, catLabel, groupLabel, groupColor) {
  const p = {};
  // Identifiants techniques (pour MapLibre + déduplication)
  p._osm_id       = osmId;
  p._osm_type     = osmType;
  p._osm_category = catKey;     // clé pour expression couleur MapLibre
  p._osm_color    = groupColor;

  // Catégorie lisible
  p.categorie = catLabel;   // ex: "Restaurants"
  p.groupe    = groupLabel; // ex: "Restaurants & Bars"

  // Nom
  p.nom = tags.name || tags["name:fr"] || tags.brand || "";

  // Adresse reconstituée
  const addrParts = [
    tags["addr:housenumber"],
    tags["addr:street"],
    tags["addr:postcode"],
    tags["addr:city"],
  ].filter(Boolean);
  if (addrParts.length) p.adresse = addrParts.join(" ");

  // Sous-catégorie (précision OSM)
  const sous = tags.cuisine || tags.sport || tags.leisure
    || tags.denomination || tags.healthcare || tags.vending || "";
  if (sous) p.sous_categorie = sous;

  // Hauteur bâtiment
  if (tags.height) {
    p.hauteur = parseFloat(tags.height) || 0;
  } else if (tags["building:levels"]) {
    p.hauteur = (parseFloat(tags["building:levels"]) || 0) * 3;
  }

  // Contact
  const tel = tags.phone || tags["contact:phone"];
  if (tel) p.telephone = tel;
  const web = tags.website || tags["contact:website"];
  if (web) p.site_web = web;

  // Horaires
  if (tags.opening_hours) p.horaires = tags.opening_hours;

  return p;
}

// ─── Overpass → GeoJSON ───────────────────────────────────────
function elementsToFeatures(elements, job) {
  const nodes = {};
  for (const e of elements) if (e.type === "node") nodes[e.id] = e;
  const features = [];
  for (const el of elements) {
    const tags = el.tags || {};
    const props = cleanProps(tags, el.id, el.type, job.key, job.label, job.groupLabel, job.groupColor);

    if (el.type === "node" && "lat" in el) {
      if (!Object.keys(tags).length) continue;
      features.push({ type: "Feature", geometry: { type: "Point", coordinates: [el.lon, el.lat] }, properties: props });
    } else if (el.center) {
      features.push({ type: "Feature", geometry: { type: "Point", coordinates: [el.center.lon, el.center.lat] }, properties: props });
    } else if (el.type === "way" && el.nodes) {
      if (!Object.keys(tags).length) continue;
      const coords = el.nodes.map(id => nodes[id]).filter(Boolean).map(n => [n.lon, n.lat]);
      if (coords.length < 2) continue;
      const closed = coords.length >= 4 && coords[0][0] === coords[coords.length-1][0] && coords[0][1] === coords[coords.length-1][1];
      features.push({ type: "Feature", geometry: closed ? { type: "Polygon", coordinates: [coords] } : { type: "LineString", coordinates: coords }, properties: props });
    }
  }
  return features;
}

// ─── Fetch Overpass avec retry ────────────────────────────────
async function overpassFetch(oql, globalSignal, timeoutMs) {
  let lastErr;
  for (const url of MIRRORS) {
    if (globalSignal?.aborted) throw new DOMException("Cancelled", "AbortError");
    const ctrl  = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    const onAbort = () => ctrl.abort();
    globalSignal?.addEventListener("abort", onAbort);
    try {
      const resp = await fetch(url, { method: "POST", body: new URLSearchParams({ data: oql }), signal: ctrl.signal });
      clearTimeout(timer); globalSignal?.removeEventListener("abort", onAbort);
      if (resp.status === 403 || resp.status === 429 || resp.status >= 500) throw new Error(`HTTP ${resp.status}`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      return await resp.json();
    } catch (e) {
      clearTimeout(timer); globalSignal?.removeEventListener("abort", onAbort);
      if (e.name === "AbortError" && globalSignal?.aborted) throw e;
      lastErr = e;
      if (!globalSignal?.aborted) await new Promise(r => setTimeout(r, 300));
    }
  }
  throw lastErr;
}

async function geocodeSuggest(query) {
  const r = await fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query)}&format=json&limit=6&addressdetails=1`, { headers: { "User-Agent": "OpenMapAgents/1.0" } });
  return (await r.json()).map(x => ({ lat: parseFloat(x.lat), lng: parseFloat(x.lon), label: x.display_name }));
}

function bboxAreaKm2(b) {
  const mid = ((b.north + b.south) / 2) * (Math.PI / 180);
  return Math.abs(b.north - b.south) * 111.32 * Math.abs(b.east - b.west) * 111.32 * Math.cos(mid);
}

// ─── Composant ───────────────────────────────────────────────
export default function OsmPanel({ layers = [], onAddLayer, mapRef }) {
  const C = useThemeContext();
  const abortRef  = useRef(null);
  const markerRef = useRef(null);

  const lbl  = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp  = { fontFamily: F, fontSize: 11, padding: "7px 10px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input, color: C.txt, width: "100%", outline: "none", boxSizing: "border-box" };
  const secH = { fontSize: 10, fontWeight: 600, color: C.mut, textTransform: "uppercase", letterSpacing: "0.07em", marginBottom: 6, marginTop: 4, display: "flex", alignItems: "center", gap: 5 };

  // Emprise
  const [empriseMode, setEmpriseMode] = useState("radius");
  const [radius,      setRadius]      = useState(500);
  const [centerLat,   setCenterLat]   = useState("");
  const [centerLng,   setCenterLng]   = useState("");
  const [addrQuery,   setAddrQuery]   = useState("");
  const [suggestions, setSuggestions] = useState([]);
  const [sugOpen,     setSugOpen]     = useState(false);
  const [geocoding,   setGeocoding]   = useState(false);
  const [centerLabel, setCenterLabel] = useState("");
  const [pickMode,    setPickMode]    = useState(false);
  const [geolocating, setGeolocating] = useState(false);
  const sugTimer = useRef(null);
  const sugRef   = useRef(null);
  const [bbox,    setBbox]    = useState({ south: "", west: "", north: "", east: "" });
  const [layerId, setLayerId] = useState("");

  // Types géo
  const [geomTypes, setGeomTypes] = useState({ poi: true, line: false, polygon: false });

  // Sélection POI par groupe
  const initGroups = () => {
    const s = {};
    POI_GROUPS.forEach(g => {
      const items = {};
      g.items.forEach(it => { items[it.key] = it.key === "restaurant"; });
      s[g.key] = { items };
    });
    return s;
  };
  const [selectedGroups,  setSelectedGroups]  = useState(initGroups);
  const [selectedLine,    setSelectedLine]    = useState({});
  const [selectedPolygon, setSelectedPolygon] = useState({});
  const [openGroups,      setOpenGroups]      = useState({ food: true });
  const [customTag,       setCustomTag]       = useState("");
  const [layerName,       setLayerName]       = useState("Données OSM");

  // État
  const [loading,     setLoading]     = useState(false);
  const [progress,    setProgress]    = useState(null);
  const [error,       setError]       = useState(null);
  const [stats,       setStats]       = useState(null);
  const [legendItems, setLegendItems] = useState([]);

  // ── Marker centre ─────────────────────────────────────────
  const placeMarker = useCallback((lat, lng) => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    markerRef.current?.remove();
    const el = document.createElement("div");
    el.innerHTML = `<svg viewBox="0 0 28 36" width="28" height="36"><path d="M14 0C6.268 0 0 6.268 0 14c0 9.333 14 22 14 22s14-12.667 14-22C28 6.268 21.732 0 14 0z" fill="#f59e0b" stroke="#fff" stroke-width="2"/><circle cx="14" cy="14" r="5" fill="#fff"/></svg>`;
    markerRef.current = new maplibregl.Marker({ element: el, anchor: "bottom" }).setLngLat([lng, lat]).addTo(map);
  }, [mapRef]);
  useEffect(() => () => markerRef.current?.remove(), []);

  const applyCenter = useCallback((lat, lng, lbStr) => {
    setCenterLat(lat.toFixed(5)); setCenterLng(lng.toFixed(5)); setCenterLabel(lbStr);
    setAddrQuery(""); setSuggestions([]); setSugOpen(false);
    placeMarker(lat, lng);
  }, [placeMarker]);

  const fillCenterFromView = useCallback(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    const c = map.getCenter();
    applyCenter(c.lat, c.lng, "Centre de la vue");
  }, [mapRef, applyCenter]);

  const doGeolocate = useCallback(() => {
    if (!navigator.geolocation) { setError("Géolocalisation non supportée."); return; }
    setGeolocating(true);
    navigator.geolocation.getCurrentPosition(
      pos => { applyCenter(pos.coords.latitude, pos.coords.longitude, "Ma position"); setGeolocating(false); mapRef?.current?.getMap?.()?.flyTo({ center: [pos.coords.longitude, pos.coords.latitude], zoom: 14, duration: 800 }); },
      ()  => { setError("Impossible d'obtenir la position."); setGeolocating(false); }
    );
  }, [mapRef, applyCenter]);

  const activatePickMode = useCallback(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    if (pickMode) { setPickMode(false); map.getCanvas().style.cursor = ""; return; }
    setPickMode(true); map.getCanvas().style.cursor = "crosshair";
    map.once("click", e => { const {lng, lat} = e.lngLat; applyCenter(lat, lng, `${lat.toFixed(4)}°, ${lng.toFixed(4)}°`); setPickMode(false); map.getCanvas().style.cursor = ""; });
  }, [mapRef, pickMode, applyCenter]);

  const onAddrChange = useCallback((val) => {
    setAddrQuery(val); setCenterLabel("");
    clearTimeout(sugTimer.current);
    if (val.trim().length < 3) { setSuggestions([]); setSugOpen(false); return; }
    setGeocoding(true);
    sugTimer.current = setTimeout(async () => {
      try { const r = await geocodeSuggest(val); setSuggestions(r); setSugOpen(r.length > 0); } catch (_) { setSuggestions([]); }
      setGeocoding(false);
    }, 350);
  }, []);

  const selectSuggestion = useCallback((sug) => {
    setAddrQuery(sug.label.split(",").slice(0,2).join(", "));
    applyCenter(sug.lat, sug.lng, sug.label.split(",").slice(0,2).join(", "));
    mapRef?.current?.getMap?.()?.flyTo({ center: [sug.lng, sug.lat], zoom: 13, duration: 800 });
  }, [mapRef, applyCenter]);

  useEffect(() => {
    const h = e => { if (sugRef.current && !sugRef.current.contains(e.target)) setSugOpen(false); };
    document.addEventListener("mousedown", h); return () => document.removeEventListener("mousedown", h);
  }, []);

  const fillBboxFromView = useCallback(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    const b = map.getBounds();
    setBbox({ south: b.getSouth().toFixed(5), west: b.getWest().toFixed(5), north: b.getNorth().toFixed(5), east: b.getEast().toFixed(5) });
  }, [mapRef]);

  const fillBboxFromLayer = useCallback((id) => {
    const lyr = layers.find(l => l.id === id);
    if (!lyr?.geojson?.features?.length) return;
    let w=Infinity, s=Infinity, e2=-Infinity, n=-Infinity;
    const walk = c => { if (typeof c[0]==="number") { if(c[0]<w)w=c[0]; if(c[0]>e2)e2=c[0]; if(c[1]<s)s=c[1]; if(c[1]>n)n=c[1]; } else c.forEach(walk); };
    lyr.geojson.features.forEach(f => f.geometry?.coordinates && walk(f.geometry.coordinates));
    setBbox({ south: s.toFixed(5), west: w.toFixed(5), north: n.toFixed(5), east: e2.toFixed(5) });
  }, [layers]);

  useEffect(() => {
    if (empriseMode === "radius" && !centerLat) { const t = setTimeout(fillCenterFromView, 200); return () => clearTimeout(t); }
    if (empriseMode === "bbox") fillBboxFromView();
  }, [empriseMode]);

  // ── Toggle POI ────────────────────────────────────────────
  const togglePoiGroup = useCallback((groupKey, groupItems) => {
    setSelectedGroups(prev => {
      const cur = prev[groupKey];
      const allOn = groupItems.every(it => cur.items[it.key]);
      const items = {}; groupItems.forEach(it => { items[it.key] = !allOn; });
      return { ...prev, [groupKey]: { items } };
    });
  }, []);

  const togglePoiItem = useCallback((groupKey, itemKey) => {
    setSelectedGroups(prev => {
      const cur = prev[groupKey];
      return { ...prev, [groupKey]: { items: { ...cur.items, [itemKey]: !cur.items[itemKey] } } };
    });
  }, []);

  // ── Jobs construits en temps réel ─────────────────────────
  const poiJobs = geomTypes.poi
    ? POI_GROUPS.flatMap(g => g.items.filter(it => selectedGroups[g.key]?.items[it.key]).map(it => ({ ...it, type: "poi", groupLabel: g.label, groupColor: g.color })))
    : [];
  const lineJobs    = geomTypes.line    ? LINE_PRESETS.filter(p    => selectedLine[p.key]).map(p    => ({ ...p, type: "line",    groupLabel: "Lignes",    groupColor: p.color })) : [];
  const polygonJobs = geomTypes.polygon ? POLYGON_PRESETS.filter(p => selectedPolygon[p.key]).map(p => ({ ...p, type: "polygon", groupLabel: "Polygones", groupColor: p.color })) : [];
  const customJobs  = customTag.trim() ? (() => {
    const parts = customTag.trim().split("=");
    const filter = parts.length >= 2 ? `"${parts[0].trim()}"="${parts.slice(1).join("=").trim()}"` : `"${parts[0].trim()}"`;
    return [{ key: "__custom__", label: customTag.trim(), q: filter, type: "poi", groupLabel: "Personnalisé", groupColor: "#805ad5" }];
  })() : [];

  const allJobs  = [...poiJobs, ...lineJobs, ...polygonJobs, ...customJobs];
  const jobCount = allJobs.length;

  // ── Import ────────────────────────────────────────────────
  const doImport = useCallback(async () => {
    setError(null); setStats(null); setProgress(null); setLegendItems([]);
    if (!allJobs.length) { setError("Sélectionnez au moins une catégorie."); return; }

    let zone, scopeLabel;
    if (empriseMode === "radius") {
      const lat = parseFloat(centerLat), lng = parseFloat(centerLng);
      if (isNaN(lat) || isNaN(lng)) { setError("Définissez un centre."); return; }
      if (radius === 0) {
        const map = mapRef?.current?.getMap?.();
        if (!map) { setError("Impossible d'obtenir la vue."); return; }
        const b = map.getBounds();
        zone = `${b.getSouth().toFixed(5)},${b.getWest().toFixed(5)},${b.getNorth().toFixed(5)},${b.getEast().toFixed(5)}`;
        scopeLabel = "vue courante";
      } else {
        zone = `around:${radius},${lat},${lng}`;
        scopeLabel = radius >= 1000 ? `${radius/1000} km` : `${radius} m`;
      }
    } else if (empriseMode === "layer") {
      const b = { south: +bbox.south, west: +bbox.west, north: +bbox.north, east: +bbox.east };
      if (Object.values(b).some(isNaN)) { setError("Emprise de couche invalide."); return; }
      zone = `${b.south},${b.west},${b.north},${b.east}`;
      scopeLabel = `couche "${layers.find(l => l.id === layerId)?.name}"`;
    } else {
      const b = { south: +bbox.south, west: +bbox.west, north: +bbox.north, east: +bbox.east };
      if (Object.values(b).some(isNaN)) { setError("Emprise bbox invalide."); return; }
      zone = `${b.south},${b.west},${b.north},${b.east}`;
      scopeLabel = `bbox ${Math.round(bboxAreaKm2(b))} km²`;
    }

    const TO_S = 55, TO_MS = 65000;
    if (abortRef.current) { abortRef.current.abort(); abortRef.current = null; await new Promise(r => setTimeout(r, 50)); }
    const ctrl = new AbortController(); abortRef.current = ctrl;
    setLoading(true);

    const runJob = async (job) => {
      if (ctrl.signal.aborted) return [];
      let oql;
      if (job.type === "poi") {
        oql = `[out:json][timeout:${TO_S}];\n(\n  node[${job.q}](${zone});\n  way[${job.q}](${zone});\n  relation[${job.q}](${zone});\n);\nout center qt;`;
      } else if (job.type === "line") {
        oql = `[out:json][timeout:${TO_S}];\n(\n  way[${job.q}](${zone});\n);\nout body;\n>;\nout skel qt;`;
      } else {
        oql = `[out:json][timeout:${TO_S}];\n(\n  way[${job.q}](${zone});\n  relation[${job.q}](${zone});\n);\nout body;\n>;\nout skel qt;`;
      }
      try {
        const data = await overpassFetch(oql, ctrl.signal, TO_MS);
        return elementsToFeatures(data.elements || [], job);
      } catch (e) {
        if (e.name === "AbortError" && ctrl.signal.aborted) throw e;
        if (e.message?.includes("408") || e.message?.includes("timeout") || e.name === "AbortError") throw new Error("TIMEOUT");
        return [];
      }
    };

    try {
      const allFeatures = []; const seen = new Set(); const legendAcc = []; let totalErrors = 0;

      for (let i = 0; i < allJobs.length; i++) {
        const job = allJobs[i];
        if (ctrl.signal.aborted) break;
        setProgress({ done: i, total: allJobs.length, current: job.label, errors: totalErrors });
        let feats = [];
        try { feats = await runJob(job); }
        catch(e) { if (e.message === "TIMEOUT") throw e; if (e.name === "AbortError") throw e; totalErrors++; }

        for (const f of feats) {
          if (!seen.has(f.properties._osm_id)) { seen.add(f.properties._osm_id); allFeatures.push(f); }
        }
        if (feats.length > 0) { legendAcc.push({ label: job.label, color: job.groupColor, count: feats.length, type: job.type, key: job.key }); setLegendItems([...legendAcc]); }
        setProgress({ done: i + 1, total: allJobs.length, current: job.label, errors: totalErrors });
      }

      if (!ctrl.signal.aborted) {
        if (allFeatures.length > 0) {
          const colorExpression = legendAcc.length > 0
            ? ["match", ["get", "_osm_category"], ...legendAcc.flatMap(l => [l.key, l.color]), "#888888"]
            : "#888888";
          const categoryColors = Object.fromEntries(legendAcc.map(l => [l.key, l.color]));
          onAddLayer(
            { type: "FeatureCollection", features: allFeatures },
            layerName || "Données OSM", "osm",
            { categoryColors, colorExpression, legend: legendAcc },
          );
        }
        setStats({ total: allFeatures.length, jobs: legendAcc.length, scope: scopeLabel, errors: totalErrors });
        if (allFeatures.length === 0 && totalErrors === 0) setError("Aucun objet trouvé dans cette zone.");
        else if (totalErrors > 0) setError(`${totalErrors} catégorie(s) ont échoué. Essayez un périmètre plus petit.`);
      }
    } catch (e) {
      if (e.message === "TIMEOUT") setError("⏱ Requête trop lente. Réduisez le rayon ou les catégories.");
      else if (e.name !== "AbortError") setError("Erreur réseau. Vérifiez votre connexion.");
    }

    setLoading(false); setProgress(null); abortRef.current = null;
  }, [allJobs, empriseMode, centerLat, centerLng, radius, bbox, layerId, layers, layerName, onAddLayer, mapRef]);

  const doCancel = () => abortRef.current?.abort();

  const areaDisplay = (() => {
    if (empriseMode === "radius") {
      if (radius === 0) return { value: "Vue courante", warn: false };
      const km2 = Math.round(Math.PI * (radius / 1000) ** 2 * 10) / 10;
      return { value: `~${km2} km²`, warn: km2 > 30 };
    }
    if (["south","west","north","east"].some(k => !bbox[k])) return null;
    const b = { south: +bbox.south, west: +bbox.west, north: +bbox.north, east: +bbox.east };
    if (Object.values(b).some(isNaN)) return null;
    return { value: `~${Math.round(bboxAreaKm2(b))} km²`, warn: bboxAreaKm2(b) > 30 };
  })();

  // ── Rendu ─────────────────────────────────────────────────
  return (
    <div style={{ flex: 1, minHeight: 0, overflowY: "auto", overflowX: "hidden", display: "flex", flexDirection: "column", padding: 12, gap: 14 }}>

      {/* ══ 1. ZONE ══ */}
      <div>
        <div style={secH}>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" width="12" height="12"><polygon points="12 2 22 19 2 19"/></svg>
          Zone d'import
        </div>
        <div style={{ display: "flex", gap: 3, marginBottom: 10 }}>
          {[["radius",IcCircleDot,"Rayon"],["bbox",IcSquare,"Bbox"],["layer",IcHexagon,"Couche"]].map(([v, Icon, l]) => (
            <button key={v} onClick={() => setEmpriseMode(v)}
              style={{ fontFamily: F, flex: 1, padding: "5px 0", borderRadius: 7, border: `0.5px solid ${empriseMode===v ? C.acc+"55" : C.bdr}`, background: empriseMode===v ? C.acc+"18" : "transparent", color: empriseMode===v ? C.acc : C.mut, cursor: "pointer", fontSize: 10, fontWeight: empriseMode===v ? 600 : 400, display: "flex", alignItems: "center", justifyContent: "center", gap: 4 }}>
              <Icon size={12}/> {l}
            </button>
          ))}
        </div>

        {empriseMode === "radius" && (
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            <div>
              <div style={{ ...lbl, display: "flex", justifyContent: "space-between" }}>
                <span>Rayon</span>
                <span style={{ fontFamily: M, color: C.acc, textTransform: "none" }}>{radius===0?"Vue":radius>=1000?`${radius/1000} km`:`${radius} m`}</span>
              </div>
              <input type="range" min={0} max={5000} step={50} value={radius} onChange={e => setRadius(+e.target.value)} style={{ width: "100%", accentColor: C.acc }} />
              <div style={{ display: "flex", flexWrap: "wrap", gap: 3, marginTop: 5 }}>
                {RADIUS_PRESETS.map(({ label: l, value: v }) => (
                  <button key={v} onClick={() => setRadius(v)} style={{ fontFamily: F, fontSize: 9, padding: "2px 7px", borderRadius: 8, border: `0.5px solid ${radius===v ? C.acc+"55" : C.bdr}`, background: radius===v ? C.acc+"18" : "transparent", color: radius===v ? C.acc : C.dim, cursor: "pointer" }}>{l}</button>
                ))}
              </div>
            </div>
            <div ref={sugRef} style={{ position: "relative" }}>
              <div style={lbl}>Centre de recherche</div>
              <div style={{ position: "relative", marginBottom: 6 }}>
                <input type="text" value={addrQuery} onChange={e => onAddrChange(e.target.value)} onFocus={() => suggestions.length > 0 && setSugOpen(true)}
                  placeholder="Rechercher une adresse…" style={{ ...inp, paddingRight: geocoding ? 30 : 10 }} />
                {geocoding && <span style={{ position: "absolute", right: 9, top: "50%", transform: "translateY(-50%)" }}>
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" width="12" height="12" style={{ animation: "spin .7s linear infinite" }}><circle cx="12" cy="12" r="10" strokeDasharray="40" strokeDashoffset="10"/></svg>
                </span>}
                {sugOpen && suggestions.length > 0 && (
                  <div style={{ position: "absolute", top: "100%", left: 0, right: 0, zIndex: 999, background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: "0 0 8px 8px", boxShadow: "0 4px 16px rgba(0,0,0,.25)", maxHeight: 200, overflowY: "auto" }}>
                    {suggestions.map((s, i) => (
                      <div key={i} onClick={() => selectSuggestion(s)}
                        style={{ padding: "7px 10px", fontSize: 11, cursor: "pointer", borderBottom: i < suggestions.length-1 ? `0.5px solid ${C.bdr}` : "none", color: C.txt }}
                        onMouseEnter={e => e.currentTarget.style.background = C.hover}
                        onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
                        <div style={{ fontWeight: 500 }}>{s.label.split(",")[0]}</div>
                        <div style={{ fontSize: 9, color: C.dim }}>{s.label.split(",").slice(1,3).join(",").trim()}</div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <div style={{ display: "flex", gap: 5 }}>
                {[
                  { onClick: activatePickMode,  icon: IcMapPin,    text: pickMode ? "Cliquez…" : "Sur la carte", active: pickMode },
                  { onClick: doGeolocate,        icon: IcCrosshair, text: geolocating ? "…" : "Ma position",    disabled: geolocating },
                  { onClick: fillCenterFromView, icon: IcSquare,    text: "Vue" },
                ].map((b, i) => (
                  <button key={i} onClick={b.onClick} disabled={b.disabled}
                    style={{ fontFamily: F, flex: 1, padding: "5px 0", borderRadius: 7, border: `0.5px solid ${b.active ? C.acc+"66" : C.bdr}`, background: b.active ? C.acc+"18" : "transparent", color: b.active ? C.acc : C.mut, cursor: b.disabled ? "default" : "pointer", fontSize: 10, display: "flex", alignItems: "center", justifyContent: "center", gap: 3, opacity: b.disabled ? 0.5 : 1 }}>
                    <b.icon size={12}/> {b.text}
                  </button>
                ))}
              </div>
              {centerLabel && <div style={{ fontSize: 10, color: C.dim, marginTop: 5, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", display: "flex", alignItems: "center", gap: 4 }}><IcMapPin size={11} style={{ flexShrink: 0 }}/> {centerLabel}</div>}
              {!centerLabel && !centerLat && <div style={{ fontSize: 10, color: "#d97706", marginTop: 5, display: "flex", alignItems: "center", gap: 4 }}><IcAlert size={11}/> Définissez un centre avant d'importer</div>}
            </div>
          </div>
        )}

        {empriseMode === "bbox" && (
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <button onClick={fillBboxFromView} style={{ fontFamily: F, fontSize: 10, padding: "5px 0", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: "transparent", color: C.mut, cursor: "pointer" }}>↺ Reprendre la vue courante</button>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 5 }}>
              {[["north","Nord"],["south","Sud"],["west","Ouest"],["east","Est"]].map(([k, l]) => (
                <div key={k}><div style={lbl}>{l}</div><input type="number" step="0.0001" value={bbox[k]} onChange={e => setBbox(b => ({ ...b, [k]: e.target.value }))} style={{ ...inp, textAlign: "center", fontFamily: M, fontSize: 11 }} /></div>
              ))}
            </div>
          </div>
        )}

        {empriseMode === "layer" && (
          <select value={layerId} onChange={e => { setLayerId(e.target.value); fillBboxFromLayer(e.target.value); }} style={inp}>
            <option value="">— Choisir une couche —</option>
            {layers.filter(l => !l.isRaster && l.geojson?.features?.length).map(l => <option key={l.id} value={l.id}>{l.name}</option>)}
          </select>
        )}

        {areaDisplay && (
          <div style={{ fontSize: 10, color: areaDisplay.warn ? "#d97706" : C.dim, marginTop: 6, textAlign: "right" }}>
            {areaDisplay.value}{areaDisplay.warn && " — zone large, risque de timeout"}
          </div>
        )}
      </div>

      {/* ══ 2. TYPES GÉOMÉTRIQUES ══ */}
      <div>
        <div style={secH}>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" width="12" height="12"><polygon points="12 2 22 19 2 19"/></svg>
          Types de géométrie
        </div>
        <div style={{ display: "flex", gap: 5 }}>
          {Object.entries(TYPE_LABELS).map(([type, tl]) => (
            <button key={type} onClick={() => { setGeomTypes(g => ({ ...g, [type]: !g[type] })); setStats(null); setLegendItems([]); setError(null); }}
              style={{ fontFamily: F, flex: 1, padding: "6px 4px", borderRadius: 7, fontSize: 10, fontWeight: 500, cursor: "pointer", border: `0.5px solid ${geomTypes[type] ? TYPE_COLORS[type]+"66" : C.bdr}`, background: geomTypes[type] ? TYPE_COLORS[type]+"14" : "transparent", color: geomTypes[type] ? TYPE_COLORS[type] : C.mut, transition: "all .12s" }}>
              {tl}
            </button>
          ))}
        </div>
      </div>

      {/* ══ 3. CATÉGORIES POI — groupes hiérarchiques ══ */}
      {geomTypes.poi && (
        <div>
          <div style={secH}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" width="12" height="12"><path d="M20.59 13.41l-7.17 7.17a2 2 0 01-2.83 0L2 12V2h10l8.59 8.59a2 2 0 010 2.82z"/><line x1="7" y1="7" x2="7.01" y2="7"/></svg>
            Points d'intérêt
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
            {POI_GROUPS.map(g => {
              const selItems = selectedGroups[g.key]?.items || {};
              const allOn  = g.items.every(it => selItems[it.key]);
              const someOn = g.items.some(it => selItems[it.key]);
              const isOpen = !!openGroups[g.key];
              const cnt    = g.items.filter(it => selItems[it.key]).length;
              return (
                <div key={g.key} style={{ border: `0.5px solid ${someOn ? g.color+"44" : C.bdr}`, borderRadius: 8, overflow: "hidden", background: someOn ? g.color+"06" : "transparent" }}>
                  <div style={{ display: "flex", alignItems: "stretch" }}>
                    {/* Checkbox + label groupe */}
                    <button onClick={() => togglePoiGroup(g.key, g.items)} style={{ padding: "8px 10px", background: "none", border: "none", cursor: "pointer", display: "flex", alignItems: "center", gap: 8, flex: 1, textAlign: "left" }}>
                      {/* Checkbox */}
                      <div style={{ width: 15, height: 15, borderRadius: 4, border: `1.5px solid ${someOn ? g.color : C.bdr}`, background: allOn ? g.color : someOn ? g.color+"33" : "transparent", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0, transition: "all .1s" }}>
                        {allOn  && <svg viewBox="0 0 10 8" width="9" height="7"><path d="M1 4l3 3 5-6" stroke="#fff" strokeWidth="1.8" fill="none" strokeLinecap="round"/></svg>}
                        {someOn && !allOn && <div style={{ width: 7, height: 2, background: g.color, borderRadius: 1 }}/>}
                      </div>
                      <span style={{ fontSize: 11, fontWeight: 600, color: someOn ? g.color : C.txt, display: "inline-flex", alignItems: "center", gap: 5 }}>{g.icon && <g.icon size={14}/>} {g.label}</span>
                      {cnt > 0 && <span style={{ fontSize: 9, color: g.color, background: g.color+"20", borderRadius: 8, padding: "1px 5px", fontWeight: 700 }}>{cnt}</span>}
                    </button>
                    {/* Bouton expand */}
                    <button onClick={() => setOpenGroups(o => ({ ...o, [g.key]: !o[g.key] }))} style={{ padding: "8px 10px", background: "none", border: "none", borderLeft: `0.5px solid ${C.bdr}`, cursor: "pointer", color: C.dim, fontSize: 12 }}>
                      {isOpen ? "▾" : "▸"}
                    </button>
                  </div>
                  {/* Sous-catégories */}
                  {isOpen && (
                    <div style={{ padding: "6px 10px 9px 34px", display: "flex", flexWrap: "wrap", gap: 4, borderTop: `0.5px solid ${C.bdr}`, background: C.hover+"88" }}>
                      {g.items.map(it => {
                        const on = !!selItems[it.key];
                        return (
                          <button key={it.key} onClick={() => togglePoiItem(g.key, it.key)}
                            style={{ fontFamily: F, fontSize: 10, padding: "3px 9px", borderRadius: 12, cursor: "pointer", border: `0.5px solid ${on ? g.color+"66" : C.bdr}`, background: on ? g.color+"18" : C.hover, color: on ? g.color : C.mut, fontWeight: on ? 600 : 400, transition: "all .1s" }}>
                            {it.label}
                          </button>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* ══ Catégories LIGNES ══ */}
      {geomTypes.line && (
        <div>
          <div style={secH}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" width="12" height="12"><line x1="3" y1="12" x2="21" y2="12"/></svg>
            Lignes
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
            {LINE_PRESETS.map(p => {
              const on = !!selectedLine[p.key];
              return <button key={p.key} onClick={() => setSelectedLine(s => ({ ...s, [p.key]: !s[p.key] }))}
                style={{ fontFamily: F, fontSize: 10, padding: "3px 9px", borderRadius: 12, cursor: "pointer", border: `0.5px solid ${on ? p.color+"66" : C.bdr}`, background: on ? p.color+"18" : C.hover, color: on ? p.color : C.mut, fontWeight: on ? 600 : 400, transition: "all .1s" }}>{p.label}</button>;
            })}
          </div>
        </div>
      )}

      {/* ══ Catégories POLYGONES ══ */}
      {geomTypes.polygon && (
        <div>
          <div style={secH}>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" width="12" height="12"><rect x="3" y="3" width="18" height="18" rx="2"/></svg>
            Polygones
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
            {POLYGON_PRESETS.map(p => {
              const on = !!selectedPolygon[p.key];
              return <button key={p.key} onClick={() => setSelectedPolygon(s => ({ ...s, [p.key]: !s[p.key] }))}
                style={{ fontFamily: F, fontSize: 10, padding: "3px 9px", borderRadius: 12, cursor: "pointer", border: `0.5px solid ${on ? p.color+"66" : C.bdr}`, background: on ? p.color+"18" : C.hover, color: on ? p.color : C.mut, fontWeight: on ? 600 : 400, transition: "all .1s" }}>{p.label}</button>;
            })}
          </div>
        </div>
      )}

      {/* ══ Tag personnalisé ══ */}
      <div>
        <div style={lbl}>Tag OSM personnalisé</div>
        <input type="text" value={customTag} onChange={e => setCustomTag(e.target.value)} placeholder='amenity=bar  ou  "name"~"Paris"' style={inp} />
      </div>

      {/* ══ Nom couche ══ */}
      <div>
        <div style={lbl}>Nom de la couche</div>
        <input type="text" value={layerName} onChange={e => setLayerName(e.target.value)} style={inp} />
      </div>

      {/* ══ Progression ══ */}
      {progress && (
        <div style={{ background: C.hover, borderRadius: 8, padding: "10px 12px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
            <span style={{ fontSize: 11, color: C.txt, fontWeight: 500, display: "flex", alignItems: "center", gap: 5 }}>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" width="12" height="12" style={{ animation: "spin .7s linear infinite", flexShrink: 0 }}><circle cx="12" cy="12" r="10" strokeDasharray="40" strokeDashoffset="10"/></svg>
              {progress.current}
            </span>
            <span style={{ fontSize: 10, color: C.dim }}>{progress.done}/{progress.total}</span>
          </div>
          <div style={{ height: 3, background: C.bdr, borderRadius: 4, overflow: "hidden" }}>
            <div style={{ height: "100%", width: `${progress.total > 0 ? (progress.done/progress.total)*100 : 0}%`, background: C.acc, borderRadius: 4, transition: "width .2s ease" }} />
          </div>
        </div>
      )}

      {/* ══ Résultat ══ */}
      {stats && (
        <div style={{ background: C.acc+"14", border: `0.5px solid ${C.acc}44`, borderRadius: 8, padding: "10px 12px" }}>
          <div style={{ fontSize: 11, fontWeight: 600, color: C.acc, marginBottom: 4, display: "flex", alignItems: "center", gap: 5 }}><IcCheck size={12}/> {stats.total.toLocaleString()} objets · {stats.scope}</div>
          <div style={{ fontSize: 10, color: C.dim }}>{stats.jobs} catégorie(s) affichée(s) sur la carte</div>
        </div>
      )}

      {/* ══ Légende ══ */}
      {legendItems.length > 0 && (
        <div style={{ background: C.hover, border: `0.5px solid ${C.bdr}`, borderRadius: 8, padding: "10px 12px" }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: C.mut, textTransform: "uppercase", letterSpacing: "0.07em", marginBottom: 8 }}>Légende</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
            {legendItems.map((item, i) => (
              <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, justifyContent: "space-between" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 7, minWidth: 0 }}>
                  {item.type === "poi"     && <svg viewBox="0 0 12 12" width="12" height="12" style={{ flexShrink: 0 }}><circle cx="6" cy="6" r="5" fill={item.color} opacity="0.9"/></svg>}
                  {item.type === "line"    && <svg viewBox="0 0 12 12" width="12" height="12" style={{ flexShrink: 0 }}><line x1="1" y1="11" x2="11" y2="1" stroke={item.color} strokeWidth="2.5" strokeLinecap="round"/></svg>}
                  {item.type === "polygon" && <svg viewBox="0 0 12 12" width="12" height="12" style={{ flexShrink: 0 }}><rect x="1" y="1" width="10" height="10" rx="2" fill={item.color} opacity="0.7" stroke={item.color} strokeWidth="1"/></svg>}
                  <span style={{ fontSize: 11, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item.label}</span>
                </div>
                <span style={{ fontSize: 10, color: C.dim, fontFamily: M, flexShrink: 0 }}>{item.count.toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ══ Erreur ══ */}
      {error && (
        <div style={{ background: "#fef2f2", border: "0.5px solid #fca5a5", borderRadius: 8, padding: "8px 10px", fontSize: 11, color: "#dc2626", display: "flex", alignItems: "center", gap: 5 }}><IcAlert size={12} style={{ flexShrink: 0 }}/> {error}</div>
      )}

      {/* ══ Bouton ══ */}
      <button onClick={loading ? doCancel : doImport}
        style={{ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "9px 0", borderRadius: 8, border: "none", background: loading ? "#dc2626" : C.acc, color: "#fff", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6, transition: "background .15s", marginTop: 2 }}>
        {loading ? (
          <><svg viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.5" width="13" height="13"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg> Annuler</>
        ) : (
          <><svg viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2" width="14" height="14"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z"/></svg>
            {jobCount > 0 ? `Importer — ${jobCount} catégorie${jobCount > 1 ? "s" : ""}` : "Importer les données OSM"}</>
        )}
      </button>

    </div>
  );
}
