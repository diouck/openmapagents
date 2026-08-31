/**
 * ShadowPanel.jsx — Ombres portées des bâtiments + canopée.
 *
 * • Bâtiments : 100 % client-side, lus des tuiles MapLibre (couche `building`,
 *   `render_height`), projetés au sol (SunCalc validé) → polygones d'ombre.
 * • Canopée : modèle WRI/Meta ~1 m via GEE (/api/shadow/canopy), rendu en
 *   aperçu raster lissé (vraie emprise, vert) ; son ombre = plusieurs copies
 *   sombres du raster empilées de la BASE jusqu'au décalage plein (pas de trou).
 * • Emprise du calcul : vue courante, emprise d'une couche, ou ROI dessiné.
 * • Statistiques des surfaces ombragées sur la zone (bouton, actif une fois la
 *   canopée chargée).
 */
import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";
import ShadowDashboard from "./ShadowDashboard";
import { importFile } from "../utils/helpers";
import { geocodeAddress } from "../utils/routing";

/* ── Position du soleil (port SunCalc, validé) → {alt, az} rad ; az depuis le
   SUD, positif vers l'ouest. */
const RAD = Math.PI / 180;
const dayMs = 86400000, J1970 = 2440588, J2000 = 2451545, OBL = RAD * 23.4397;
const toDays = (ms) => ms / dayMs - 0.5 + J1970 - J2000;
const solarMeanAnomaly = (d) => RAD * (357.5291 + 0.98560028 * d);
const eclipticLongitude = (M) => {
  const C = RAD * (1.9148 * Math.sin(M) + 0.02 * Math.sin(2 * M) + 0.0003 * Math.sin(3 * M));
  return M + C + RAD * 102.9372 + Math.PI;
};
const declination = (l) => Math.asin(Math.sin(OBL) * Math.sin(l));
const rightAscension = (l) => Math.atan2(Math.sin(l) * Math.cos(OBL), Math.cos(l));
const siderealTime = (d, lw) => RAD * (280.16 + 360.9856235 * d) - lw;
function sunPosition(ms, lat, lng) {
  const lw = RAD * -lng, phi = RAD * lat, d = toDays(ms);
  const L = eclipticLongitude(solarMeanAnomaly(d));
  const dec = declination(L), H = siderealTime(d, lw) - rightAscension(L);
  const alt = Math.asin(Math.sin(phi) * Math.sin(dec) + Math.cos(phi) * Math.cos(dec) * Math.cos(H));
  const az = Math.atan2(Math.sin(H), Math.cos(H) * Math.sin(phi) - Math.tan(dec) * Math.cos(phi));
  return { alt, az };
}
/* Heure CIVILE locale (fuseau du navigateur, DST géré) → epoch ms. Le curseur
   « hour » est donc l'heure de la montre, pas l'heure solaire. */
function localMs(dateStr, hour, offsetHours) {
  const [y, mo, d] = dateStr.split("-").map(Number);
  return Date.UTC(y, mo - 1, d, 0, 0, 0, 0) + (hour - offsetHours) * 3600 * 1000;
}
/* Décalage horaire (heures) selon le mode : longitude (défaut, correct hors
   Europe p.ex. US), UTC, ou fuseau du navigateur. */
function tzOffsetHours(lng, mode) {
  if (mode === "utc") return 0;
  if (mode === "browser") return -new Date().getTimezoneOffset() / 60;
  return Math.round(lng / 15);   // auto : heure solaire moyenne du lieu
}

/* Lever / coucher du soleil (port SunCalc getTimes) pour un lieu + un jour.
   Renvoie {rise, set} en epoch ms, ou null (jour/nuit polaire). */
const J0 = 0.0009;
const julianCycle = (d, lw) => Math.round(d - J0 - lw / (2 * Math.PI));
const approxTransit = (Ht, lw, n) => J0 + (Ht + lw) / (2 * Math.PI) + n;
const solarTransitJ = (ds, M, L) => J2000 + ds + 0.0053 * Math.sin(M) - 0.0069 * Math.sin(2 * L);
const hourAngleH = (h, phi, d) => Math.acos((Math.sin(h) - Math.sin(phi) * Math.sin(d)) / (Math.cos(phi) * Math.cos(d)));
const fromJulianMs = (j) => (j + 0.5 - J1970) * dayMs;
function sunTimesMs(dateMs, lat, lng) {
  const lw = RAD * -lng, phi = RAD * lat, d = toDays(dateMs);
  const n = julianCycle(d, lw);
  const ds = approxTransit(0, lw, n);
  const M = solarMeanAnomaly(ds);
  const L = eclipticLongitude(M);
  const dec = declination(L);
  const Jnoon = solarTransitJ(ds, M, L);
  const w = hourAngleH(-0.833 * RAD, phi, dec);
  if (isNaN(w)) return { rise: null, set: null };          // soleil de minuit / nuit polaire
  const Jset = solarTransitJ(approxTransit(w, lw, n), M, L);
  const Jrise = Jnoon - (Jset - Jnoon);
  return { rise: fromJulianMs(Jrise), set: fromJulianMs(Jset) };
}
const pad2 = (x) => String(x).padStart(2, "0");
/* Formate une heure décimale (0..24) en HH:MM. */
const fmtH = (h) => { const t = Math.round((((h % 24) + 24) % 24) * 60); return `${pad2(Math.floor(t / 60) % 24)}:${pad2(t % 60)}`; };
const fmtOffset = (o) => `UTC${o >= 0 ? "+" : "−"}${Math.abs(o)}`;

function convexHull(pts) {
  if (pts.length < 3) return pts;
  const p = pts.slice().sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  const cross = (o, a, b) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
  const lo = [];
  for (const q of p) { while (lo.length >= 2 && cross(lo[lo.length - 2], lo[lo.length - 1], q) <= 0) lo.pop(); lo.push(q); }
  const up = [];
  for (let i = p.length - 1; i >= 0; i--) { const q = p[i]; while (up.length >= 2 && cross(up[up.length - 2], up[up.length - 1], q) <= 0) up.pop(); up.push(q); }
  lo.pop(); up.pop(); return lo.concat(up);
}

function queryTiles(map, name) {
  const sl = map.getStyle().layers || [];
  const lyr = sl.find((l) => l["source-layer"] === name && l.source);
  try {
    let f = lyr ? map.querySourceFeatures(lyr.source, { sourceLayer: name }) : [];
    if (!f.length) f = map.querySourceFeatures("openmaptiles", { sourceLayer: name });
    return f || [];
  } catch (_) { return []; }
}

function layerBbox(l) {
  if (Array.isArray(l.bbox) && l.bbox.length === 4) return l.bbox;
  const gj = l.geojson; if (!gj?.features?.length) return null;
  let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
  const scan = (c) => {
    if (typeof c[0] === "number") { w = Math.min(w, c[0]); e = Math.max(e, c[0]); s = Math.min(s, c[1]); n = Math.max(n, c[1]); }
    else for (const k of c) scan(k);
  };
  for (const f of gj.features) if (f.geometry?.coordinates) scan(f.geometry.coordinates);
  return isFinite(w) ? [w, s, e, n] : null;
}

/* ── Graphe piéton léger depuis les tuiles + Dijkstra pondéré par l'ombre ──── */
class MinHeap {
  constructor() { this.a = []; }
  get size() { return this.a.length; }
  push(k, p) { const a = this.a; a.push([k, p]); let i = a.length - 1; while (i > 0) { const par = (i - 1) >> 1; if (a[par][1] <= a[i][1]) break; [a[par], a[i]] = [a[i], a[par]]; i = par; } }
  pop() { const a = this.a, top = a[0], last = a.pop(); if (a.length) { a[0] = last; let i = 0; const n = a.length; for (;;) { let l = 2 * i + 1, r = 2 * i + 2, s = i; if (l < n && a[l][1] < a[s][1]) s = l; if (r < n && a[r][1] < a[s][1]) s = r; if (s === i) break;[a[s], a[i]] = [a[i], a[s]]; i = s; } } return top; }
}
const nodeKey = (p) => `${p[0].toFixed(6)},${p[1].toFixed(6)}`;
/* segments = [[ [lng,lat]… ]…] ; sampler.shaded(lng,lat) → ombre 0/1 par arête. */
function buildGraph(segments, sampler) {
  const nodes = new Map(), adj = new Map();
  const addNode = (p) => { const k = nodeKey(p); if (!nodes.has(k)) { nodes.set(k, p); adj.set(k, []); } return k; };
  // fraction d'ombre de l'arête = moyenne sur plusieurs points (0..1)
  const edgeShade = (a, b) => { let c = 0; const fr = [0.15, 0.4, 0.6, 0.85]; for (const f of fr) if (sampler.shaded(a[0] + (b[0] - a[0]) * f, a[1] + (b[1] - a[1]) * f)) c++; return c / fr.length; };
  for (const seg of segments) {
    for (let i = 0; i < seg.length - 1; i++) {
      const a = seg[i], b = seg[i + 1], len = haversine(a, b); if (!(len > 0)) continue;
      const ka = addNode(a), kb = addNode(b), sh = edgeShade(a, b);
      adj.get(ka).push({ to: kb, len, shade: sh }); adj.get(kb).push({ to: ka, len, shade: sh });
    }
  }
  return { nodes, adj };
}
function dijkstra(graph, startKey, endKey, weightFn) {
  const dist = new Map([[startKey, 0]]), prev = new Map(), seen = new Set(), heap = new MinHeap();
  heap.push(startKey, 0);
  while (heap.size) {
    const [u, du] = heap.pop(); if (seen.has(u)) continue; seen.add(u); if (u === endKey) break;
    for (const e of graph.adj.get(u) || []) {
      const nd = du + weightFn(e); if (nd < (dist.get(e.to) ?? Infinity)) { dist.set(e.to, nd); prev.set(e.to, u); heap.push(e.to, nd); }
    }
  }
  if (startKey !== endKey && !prev.has(endKey)) return null;
  const path = []; let cur = endKey; const guard = graph.nodes.size + 2; let g = 0;
  while (cur !== undefined && g++ < guard) { const p = graph.nodes.get(cur); if (p) path.push(p); if (cur === startKey) break; cur = prev.get(cur); }
  return path.reverse();
}

const SRC = "oma-shadow-src", LYR = "oma-shadow-fill";
const IMG_DISP = "oma-canopy-img";
const SHAD_K = 6;                              // copies d'ombre canopée (base→plein)
const shadId = (i) => `oma-canopy-shad-${i}`;
const ROI_SRC = "oma-roi-src", ROI_LYR = "oma-roi-line";
const ZONE_SRC = "oma-zone-src", ZONE_FILL = "oma-zone-fill", ZONE_LINE = "oma-zone-line";
const RT_SRC = "oma-route-src", RT_LINE = "oma-route-line", RT_AB = "oma-route-ab", RT_MARK = "oma-route-mark";
const MAX_BLD = 4000, BLD_ZOOM = 16;

/* Distance géodésique (m) entre deux [lng,lat]. */
function haversine(a, b) {
  const R = 6371000, la1 = a[1] * RAD, la2 = b[1] * RAD;
  const x = Math.sin((b[1] - a[1]) * RAD / 2) ** 2 + Math.cos(la1) * Math.cos(la2) * Math.sin((b[0] - a[0]) * RAD / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(x)));
}
/* Densifie une polyligne à ~stepM mètres (pour échantillonner l'ombre). */
function densify(coords, stepM) {
  const out = [];
  for (let i = 0; i < coords.length - 1; i++) {
    const a = coords[i], b = coords[i + 1], d = haversine(a, b), n = Math.max(1, Math.floor(d / stepM));
    for (let k = 0; k < n; k++) { const t = k / n; out.push([a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]); }
  }
  out.push(coords[coords.length - 1]);
  return out;
}
/* Distances cumulées le long d'une polyligne. */
function cumDist(coords) { const c = [0]; for (let i = 1; i < coords.length; i++) c.push(c[i - 1] + haversine(coords[i - 1], coords[i])); return c; }
/* Position à la fraction f (0..1) de la longueur. */
function alongRoute(coords, cum, f) {
  const total = cum[cum.length - 1] || 1, target = f * total;
  let i = 1; while (i < cum.length && cum[i] < target) i++;
  if (i >= cum.length) return coords[coords.length - 1];
  const seg = cum[i] - cum[i - 1], t = seg > 0 ? (target - cum[i - 1]) / seg : 0, a = coords[i - 1], b = coords[i];
  return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
}
/* Cap (degrés depuis le nord) de a vers b. */
function bearingDeg(a, b) {
  const y = Math.sin((b[0] - a[0]) * RAD) * Math.cos(b[1] * RAD);
  const x = Math.cos(a[1] * RAD) * Math.sin(b[1] * RAD) - Math.sin(a[1] * RAD) * Math.cos(b[1] * RAD) * Math.cos((b[0] - a[0]) * RAD);
  return (Math.atan2(y, x) / RAD + 360) % 360;
}
/* Icône flèche de navigation (ImageData) pour le repère qui avance. */
function navArrowImage(size = 48) {
  const cv = document.createElement("canvas"); cv.width = size; cv.height = size;
  const ctx = cv.getContext("2d"); ctx.translate(size / 2, size / 2);
  ctx.beginPath();
  ctx.moveTo(0, -size * 0.40); ctx.lineTo(size * 0.28, size * 0.32); ctx.lineTo(0, size * 0.12); ctx.lineTo(-size * 0.28, size * 0.32);
  ctx.closePath();
  ctx.fillStyle = "#2563eb"; ctx.fill();
  ctx.lineWidth = size * 0.07; ctx.strokeStyle = "#fff"; ctx.stroke();
  return ctx.getImageData(0, 0, size, size);
}

/* Polygones [ [ [lng,lat]… ] …] extraits d'un GeoJSON (anneaux extérieurs). */
function extractPolys(gj) {
  const out = [];
  for (const f of gj.features || []) {
    const g = f.geometry; if (!g) continue;
    if (g.type === "Polygon") out.push(g.coordinates[0]);
    else if (g.type === "MultiPolygon") for (const p of g.coordinates) out.push(p[0]);
  }
  return out;
}
const loadImage = (url) => new Promise((res, rej) => { const im = new Image(); im.onload = () => res(im); im.onerror = rej; im.src = url; });

/* Point dans l'un des anneaux (ray casting) — filtre à l'emprise exacte. */
function pointInPolys(lng, lat, polys) {
  for (const ring of polys) {
    let inside = false;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const xi = ring[i][0], yi = ring[i][1], xj = ring[j][0], yj = ring[j][1];
      if (((yi > lat) !== (yj > lat)) && (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi)) inside = !inside;
    }
    if (inside) return true;
  }
  return false;
}
/* Géométrie MultiPolygon (pour le clip GEE de la canopée). */
function zoneGeometry(gj) {
  const polys = [];
  for (const f of gj.features || []) { const g = f.geometry; if (!g) continue; if (g.type === "Polygon") polys.push(g.coordinates); else if (g.type === "MultiPolygon") for (const p of g.coordinates) polys.push(p); }
  return polys.length ? { type: "MultiPolygon", coordinates: polys } : null;
}

export default function ShadowPanel({ mapRef, layers = [], basemap, setBasemap }) {
  const C = useThemeContext();
  const today = new Date().toISOString().slice(0, 10);
  const [tab, setTab] = useState("sim");
  const [date, setDate] = useState(today);
  const [hour, setHour] = useState(14);
  const [opacity, setOpacity] = useState(0.35);
  const [defH, setDefH] = useState(9);
  const [trees, setTrees] = useState(true);
  const [playing, setPlaying] = useState(false);
  const [scope, setScope] = useState("view");
  const [roiReady, setRoiReady] = useState(false);
  const [roiDrawing, setRoiDrawing] = useState(false);
  const [info, setInfo] = useState(null);
  const [canopyMsg, setCanopyMsg] = useState(null);
  const [sunTimes, setSunTimes] = useState(null);   // {riseH, setH, riseStr, setStr, polar, off}
  const [tzMode, setTzMode] = useState("auto");     // "auto" (longitude) | "utc" | "browser"
  const [navMode, setNavMode] = useState("immersive"); // "immersive" | "top" | "follow"
  const [zoneName, setZoneName] = useState(null);   // nom du GeoJSON importé
  const [dashData, setDashData] = useState(null);   // {data, meta} du tableau de bord
  const [dashBusy, setDashBusy] = useState(false);
  // itinéraire ombragé
  const [routeAB, setRouteAB] = useState([]);       // points A/B (affichage)
  const [routePick, setRoutePick] = useState(false);
  const [routeBusy, setRouteBusy] = useState(false);
  const [routeResult, setRouteResult] = useState(null);   // {shade:{...}, direct:{...}}
  const [routeSel, setRouteSel] = useState("shade");      // "shade" | "direct"
  const [routeErr, setRouteErr] = useState(null);
  const [previewing, setPreviewing] = useState(false);
  const [previewSpeed, setPreviewSpeed] = useState(2);
  const [addr, setAddr] = useState({ a: "", b: "" });     // adresses saisies A/B
  const [sugg, setSugg] = useState({ a: [], b: [] });     // suggestions autocomplétion

  const bldRef = useRef([]);
  const featsRef = useRef([]);
  const canopyRef = useRef(null);       // { url, corners, meanH, areaM2 }
  const zoneRef = useRef(null);         // { geojson, bbox, name } (GeoJSON importé)
  const zonePolysRef = useRef(null);    // anneaux du GeoJSON (filtre exact) ou null
  const fileRef = useRef(null);
  const treesRef = useRef(true);
  const canopyTimer = useRef(null);
  const scopeBboxRef = useRef(null);
  const roiBboxRef = useRef(null);
  const roiHandlerRef = useRef(null);
  const roiPtsRef = useRef([]);
  const prevBaseRef = useRef(null);
  const prevPitchRef = useRef(null);
  const playRef = useRef(null);
  const sunRef = useRef({ riseH: 6, setH: 21 });   // lever/coucher (heures locales) pour la lecture
  const routeABRef = useRef([]);
  const routeClickRef = useRef(null);
  const canImgRef = useRef(null);        // { url, img } — cache image canopée
  const routeSelRef = useRef("shade");
  const routeGeomRef = useRef(null);     // { shade:{coords,cum}, direct:{coords,cum} }
  const animRef = useRef(null);          // { raf, start }
  const previewSpeedRef = useRef(2);
  const preCamRef = useRef(null);        // caméra avant prévisualisation (pour restaurer)
  const geoTimer = useRef(null);         // debounce géocodage
  const tzModeRef = useRef("auto");
  const navModeRef = useRef("immersive");
  tzModeRef.current = tzMode; navModeRef.current = navMode;   // synchro (lecture dans les callbacks)

  // ── ouverture : Liberty + 3D + zoom ; restauré à la sortie ────────────────
  useEffect(() => {
    prevBaseRef.current = basemap;
    if (basemap !== "liberty") setBasemap?.("liberty");
    const map = mapRef?.current?.getMap?.();
    if (map) {
      prevPitchRef.current = map.getPitch();
      const opts = { pitch: 55, duration: 900 };
      if (map.getZoom() < 15) opts.zoom = BLD_ZOOM;
      try { map.easeTo(opts); } catch (_) {}
    }
    return () => {
      if (prevBaseRef.current && prevBaseRef.current !== "liberty") setBasemap?.(prevBaseRef.current);
      const m = mapRef?.current?.getMap?.();
      try { if (m && prevPitchRef.current != null) m.easeTo({ pitch: prevPitchRef.current, duration: 600 }); } catch (_) {}
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const layerOptions = useMemo(() =>
    layers.filter((l) => l && (Array.isArray(l.bbox) || l.geojson?.features?.length))
      .map((l) => ({ id: l.id, name: l.name, bbox: layerBbox(l) }))
      .filter((o) => o.bbox), [layers]);

  const beforeId = (map) => {
    const sl = map.getStyle().layers || [];
    return (sl.find((l) => l.type === "fill-extrusion") || sl.find((l) => l.type === "symbol"))?.id;
  };
  const setVis = (map, id, on) => { try { if (map.getLayer(id)) map.setLayoutProperty(id, "visibility", on ? "visible" : "none"); } catch (_) {} };

  const ensureShadowLayer = useCallback((map) => {
    if (!map.getSource(SRC)) map.addSource(SRC, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    if (!map.getLayer(LYR)) {
      map.addLayer({ id: LYR, type: "fill", source: SRC,
        paint: { "fill-color": "#0e1630", "fill-opacity": opacity, "fill-antialias": false } }, beforeId(map));
    }
  }, [opacity]);

  // canopée : K copies d'ombre (base→plein) + affichage vert par-dessus
  const ensureCanopyLayers = useCallback((map, url, corners) => {
    const before = beforeId(map);
    for (let i = 0; i < SHAD_K; i++) {
      const id = shadId(i);
      if (!map.getSource(id)) map.addSource(id, { type: "image", url, coordinates: corners });
      else map.getSource(id).updateImage({ url, coordinates: corners });
      if (!map.getLayer(id)) map.addLayer({ id, type: "raster", source: id,
        paint: { "raster-opacity": 0.17, "raster-brightness-max": 0.2, "raster-saturation": -0.4, "raster-fade-duration": 0 } }, before);
    }
    if (!map.getSource(IMG_DISP)) map.addSource(IMG_DISP, { type: "image", url, coordinates: corners });
    else map.getSource(IMG_DISP).updateImage({ url, coordinates: corners });
    if (!map.getLayer(IMG_DISP)) map.addLayer({ id: IMG_DISP, type: "raster", source: IMG_DISP,
      paint: { "raster-opacity": 0.82, "raster-fade-duration": 0 } }, before);
  }, []);

  const refreshBuildings = useCallback((map, bbox) => {
    const inB = bbox ? (lng, lat) => lng >= bbox[0] && lng <= bbox[2] && lat >= bbox[1] && lat <= bbox[3] : null;
    const zp = zonePolysRef.current;   // filtre à l'emprise exacte (GeoJSON importé)
    const seen = new Set();
    const out = [];
    for (const f of queryTiles(map, "building")) {
      if (out.length >= MAX_BLD) break;
      const id = f.id != null ? f.id : null;
      if (id != null) { if (seen.has(id)) continue; seen.add(id); }
      const p = f.properties || {};
      const g = f.geometry; if (!g) continue;
      let hh = Number(p.render_height ?? p.height);
      if (!isFinite(hh) || hh <= 0) hh = NaN;
      const polys = g.type === "Polygon" ? [g.coordinates] : g.type === "MultiPolygon" ? g.coordinates : [];
      for (const poly of polys) {
        const ring = poly[0]; if (!ring || ring.length < 4) continue;
        if (inB && !inB(ring[0][0], ring[0][1])) continue;
        if (zp && !pointInPolys(ring[0][0], ring[0][1], zp)) continue;
        // hf = enveloppe convexe de l'emprise, précalculée → ombre projetée plus légère
        out.push({ hf: convexHull(ring), h: hh, lat: ring[0][1] });
      }
    }
    bldRef.current = out;
    return out;
  }, []);

  const compute = useCallback(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    ensureShadowLayer(map);
    let blds = bldRef.current;
    if (!blds.length) blds = refreshBuildings(map, scopeBboxRef.current);

    const c = map.getCenter();
    const { alt, az } = sunPosition(localMs(date, Number(hour), tzOffsetHours(c.lng, tzModeRef.current)), c.lat, c.lng);
    const altDeg = alt / RAD;
    const src = map.getSource(SRC);
    const can = canopyRef.current;
    const night = alt <= 0.02;

    setVis(map, IMG_DISP, trees && !!can);
    for (let i = 0; i < SHAD_K; i++) setVis(map, shadId(i), false);

    if (night) {
      src && src.setData({ type: "FeatureCollection", features: [] });
      featsRef.current = [];
      setInfo({ night: true, alt: altDeg, count: 0 });
      return;
    }

    const bearing = ((az / RAD) % 360 + 360) % 360;
    const th = bearing * RAD;
    const factor = 1 / Math.tan(alt);
    const cosN = Math.cos(th), sinE = Math.sin(th);

    // ombres bâtiments (vecteur)
    const feats = [];
    for (const b of blds) {
      const H = isFinite(b.h) ? b.h : Number(defH);
      if (!(H > 0)) continue;
      const d = H * factor;
      const dLat = (d * cosN) / 111320, dLng = (d * sinE) / (111320 * Math.cos(b.lat * RAD));
      const hf = b.hf, m = hf.length, pts = new Array(m * 2);
      for (let i = 0; i < m; i++) { const q = hf[i]; pts[i] = q; pts[m + i] = [q[0] + dLng, q[1] + dLat]; }
      const hull = convexHull(pts);
      if (hull.length < 3) continue;
      hull.push(hull[0]);
      feats.push({ type: "Feature", properties: null, geometry: { type: "Polygon", coordinates: [hull] } });
    }
    src && src.setData({ type: "FeatureCollection", features: feats });
    featsRef.current = feats;

    // ombre de canopée : copies empilées de la base (0) au décalage plein
    if (trees && can && can.meanH > 0) {
      const full = can.meanH * factor;
      for (let i = 0; i < SHAD_K; i++) {
        const frac = SHAD_K > 1 ? i / (SHAD_K - 1) : 1;
        const d = full * frac;
        const dLat = (d * cosN) / 111320;
        const shad = can.corners.map(([lng, lat]) => [lng + (d * sinE) / (111320 * Math.cos(lat * RAD)), lat + dLat]);
        const ss = map.getSource(shadId(i));
        if (ss) { try { ss.setCoordinates(shad); } catch (_) {} setVis(map, shadId(i), true); }
      }
    }
    setInfo({ night: false, alt: altDeg, factor, count: feats.length });
  }, [mapRef, date, hour, defH, trees, ensureShadowLayer, refreshBuildings]);

  useEffect(() => { const t = requestAnimationFrame(compute); return () => cancelAnimationFrame(t); }, [compute]);

  // ── lever/coucher du soleil pour le lieu (centre carte) + le jour ─────────
  const refreshSun = useCallback(() => {
    const map = mapRef?.current?.getMap?.(); if (!map) return;
    const c = map.getCenter();
    const [y, mo, dd] = date.split("-").map(Number);
    const noon = Date.UTC(y, mo - 1, dd, 12, 0, 0, 0);
    const off = tzOffsetHours(c.lng, tzModeRef.current);
    const t = sunTimesMs(noon, c.lat, c.lng);
    if (!t.rise || !t.set) { setSunTimes({ polar: true, off }); sunRef.current = { riseH: 0, setH: 24 }; return; }
    const dayStart = Date.UTC(y, mo - 1, dd, 0, 0, 0, 0);
    const riseH = (t.rise - dayStart) / 3600e3 + off;   // heure locale (mode fuseau)
    const setH = (t.set - dayStart) / 3600e3 + off;
    sunRef.current = { riseH, setH };
    setSunTimes({ riseH, setH, riseStr: fmtH(riseH), setStr: fmtH(setH), off });
  }, [mapRef, date, tzMode]);
  useEffect(() => { refreshSun(); }, [refreshSun]);
  // le changement de fuseau modifie l'heure UTC → recalcule ombres + soleil
  useEffect(() => { const t = requestAnimationFrame(() => { compute(); refreshSun(); }); return () => cancelAnimationFrame(t); // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tzMode]);

  // ── Canopée Meta (raster lissé) ───────────────────────────────────────────
  const fetchCanopy = useCallback(async () => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    let bbox = scopeBboxRef.current;
    if (!bbox) { const b = map.getBounds(); if (!b) return; bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]; }
    if ((bbox[2] - bbox[0]) * (bbox[3] - bbox[1]) > 0.2) {
      canopyRef.current = null; setVis(map, IMG_DISP, false); for (let i = 0; i < SHAD_K; i++) setVis(map, shadId(i), false);
      setCanopyMsg({ err: "Zoomez pour charger la canopée (emprise trop grande)." }); return;
    }
    setCanopyMsg({ busy: true });
    try {
      const body = { bbox, min_height: 3 };
      if (zonePolysRef.current && zoneRef.current?.geojson) { const gm = zoneGeometry(zoneRef.current.geojson); if (gm) body.geometry = gm; }
      const r = await fetch(`${API}/shadow/canopy`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      const url = `data:image/png;base64,${d.canopy_b64}`;
      canopyRef.current = { url, corners: d.image_coordinates, meanH: d.mean_height, areaM2: d.canopy_area_m2 };
      ensureCanopyLayers(map, url, d.image_coordinates);
      setCanopyMsg({ ok: true, dataset: d.dataset, meanH: d.mean_height, areaM2: d.canopy_area_m2 });
      compute();
    } catch (e) {
      canopyRef.current = null; setVis(map, IMG_DISP, false); for (let i = 0; i < SHAD_K; i++) setVis(map, shadId(i), false);
      setCanopyMsg({ err: e.message || String(e) }); compute();
    }
  }, [mapRef, compute, ensureCanopyLayers]);

  const scheduleCanopy = useCallback(() => {
    clearTimeout(canopyTimer.current);
    canopyTimer.current = setTimeout(() => fetchCanopy(), 400);
  }, [fetchCanopy]);

  useEffect(() => {
    treesRef.current = trees;
    if (trees) scheduleCanopy();
    else {
      const map = mapRef?.current?.getMap?.();
      if (map) { setVis(map, IMG_DISP, false); for (let i = 0; i < SHAD_K; i++) setVis(map, shadId(i), false); }
      setCanopyMsg(null); compute();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [trees]);

  useEffect(() => {
    const map = mapRef?.current?.getMap?.();
    if (map && map.getLayer(LYR)) { try { map.setPaintProperty(LYR, "fill-opacity", Number(opacity)); } catch (_) {} }
  }, [opacity, mapRef]);

  // ── Emprise de calcul : vue / couche / ROI ────────────────────────────────
  const clearRoi = useCallback((map) => {
    roiBboxRef.current = null; setRoiReady(false);
    const cs = map?.getSource?.(ROI_SRC); if (cs) cs.setData({ type: "FeatureCollection", features: [] });
  }, []);

  const changeScope = useCallback((val) => {
    setScope(val);
    const map = mapRef?.current?.getMap?.();
    if (val === "view") {
      scopeBboxRef.current = null; zonePolysRef.current = null; if (map) clearRoi(map);
      if (map) { refreshBuildings(map, null); compute(); if (treesRef.current) scheduleCanopy(); }
      return;
    }
    if (val === "roi") {
      scopeBboxRef.current = roiBboxRef.current; zonePolysRef.current = null;
      if (map && roiBboxRef.current) { const [w, s, e, n] = roiBboxRef.current; map.fitBounds([[w, s], [e, n]], { padding: 60, duration: 700 }); if (treesRef.current) scheduleCanopy(); }
      return;
    }
    if (val === "import") {
      scopeBboxRef.current = zoneRef.current?.bbox || null;
      zonePolysRef.current = zoneRef.current?.geojson ? extractPolys(zoneRef.current.geojson) : null;
      if (map && zoneRef.current?.bbox) { const [w, s, e, n] = zoneRef.current.bbox; map.fitBounds([[w, s], [e, n]], { padding: 50, duration: 700 }); if (treesRef.current) scheduleCanopy(); }
      return;
    }
    const opt = layerOptions.find((o) => o.id === val);
    scopeBboxRef.current = opt?.bbox || null; zonePolysRef.current = null; if (map) clearRoi(map);
    if (map && opt?.bbox) { const [w, s, e, n] = opt.bbox; map.fitBounds([[w, s], [e, n]], { padding: 40, duration: 800 }); if (treesRef.current) scheduleCanopy(); }
  }, [mapRef, layerOptions, refreshBuildings, compute, scheduleCanopy, clearRoi]);

  // dessine la zone GeoJSON importée (contour + léger remplissage)
  const drawZone = useCallback((map, gj) => {
    if (!map.getSource(ZONE_SRC)) map.addSource(ZONE_SRC, { type: "geojson", data: gj });
    else map.getSource(ZONE_SRC).setData(gj);
    const before = beforeId(map);
    if (!map.getLayer(ZONE_FILL)) map.addLayer({ id: ZONE_FILL, type: "fill", source: ZONE_SRC, paint: { "fill-color": "#e8590c", "fill-opacity": 0.06 } }, before);
    if (!map.getLayer(ZONE_LINE)) map.addLayer({ id: ZONE_LINE, type: "line", source: ZONE_SRC, paint: { "line-color": "#e8590c", "line-width": 2 } }, before);
  }, []);

  const onImportZone = useCallback(async (file) => {
    if (!file) return;
    try {
      const gj = await importFile(file);
      const bbox = layerBbox({ geojson: gj });
      if (!bbox) throw new Error("aucune géométrie exploitable");
      zoneRef.current = { geojson: gj, bbox, name: file.name };
      zonePolysRef.current = extractPolys(gj);   // filtre + clip à l'emprise exacte
      setZoneName(file.name);
      const map = mapRef?.current?.getMap?.();
      scopeBboxRef.current = bbox; setScope("import");
      if (map) {
        drawZone(map, gj); clearRoi(map);
        const [w, s, e, n] = bbox; map.fitBounds([[w, s], [e, n]], { padding: 50, duration: 700 });
        refreshBuildings(map, bbox); compute(); if (treesRef.current) scheduleCanopy();
      }
    } catch (e) { setCanopyMsg({ err: `Import zone : ${e.message || e}` }); }
  }, [mapRef, drawZone, clearRoi, refreshBuildings, compute, scheduleCanopy]);

  // ── Tableau de bord : ombrage de la zone sur toute la journée ─────────────
  const computeDaily = useCallback(async () => {
    const map = mapRef?.current?.getMap?.(); if (!map) return;
    let bbox = scopeBboxRef.current;
    if (!bbox) { const b = map.getBounds(); bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]; }
    setDashBusy(true);
    try {
      const blds = refreshBuildings(map, bbox);
      const [w, s, e, n] = bbox;
      const midlat = ((s + n) / 2) * RAD;
      const bboxArea = Math.abs((e - w) * 111320 * Math.cos(midlat) * (n - s) * 111320);
      const W = 440, Hh = Math.max(1, Math.min(1000, Math.round(W * (n - s) / (e - w))));
      const X = (lng) => (lng - w) / (e - w) * W, Y = (lat) => (n - lat) / (n - s) * Hh;
      // masque de zone
      const cvZ = document.createElement("canvas"); cvZ.width = W; cvZ.height = Hh;
      const zx = cvZ.getContext("2d"); zx.fillStyle = "#fff";
      const zonePolys = (scope === "import" && zoneRef.current?.geojson) ? extractPolys(zoneRef.current.geojson) : [[[w, s], [e, s], [e, n], [w, n], [w, s]]];
      for (const ring of zonePolys) { zx.beginPath(); ring.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]); i ? zx.lineTo(x, y) : zx.moveTo(x, y); }); zx.closePath(); zx.fill(); }
      const zoneA = zx.getImageData(0, 0, W, Hh).data;
      let zonePix = 0; for (let i = 3; i < zoneA.length; i += 4) if (zoneA[i] > 0) zonePix++;
      const zoneArea = (zonePix / (W * Hh)) * bboxArea;

      let canImg = null, meanH = 0, canPctStatic = 0;
      if (trees && canopyRef.current) {
        try { canImg = await loadImage(canopyRef.current.url); } catch (_) { canImg = null; }
        meanH = canopyRef.current.meanH || 0;
        canPctStatic = zoneArea ? (canopyRef.current.areaM2 || 0) / zoneArea * 100 : 0;
      }
      const cvB = document.createElement("canvas"); cvB.width = W; cvB.height = Hh;
      const cvC = document.createElement("canvas"); cvC.width = W; cvC.height = Hh;
      const bx = cvB.getContext("2d"), cx = cvC.getContext("2d");
      const cLng = (w + e) / 2, cLat = (s + n) / 2;

      const lo = Math.max(0, Math.floor(sunRef.current?.riseH ?? 6));
      const hi = Math.min(24, Math.ceil(sunRef.current?.setH ?? 20));
      const out = [];
      for (let hr = lo; hr <= hi; hr++) {
        const { alt, az } = sunPosition(localMs(date, hr, tzOffsetHours(cLng, tzModeRef.current)), cLat, cLng);
        if (alt <= 0.02) { out.push({ hour: hr, alt: alt / RAD, night: true, bldPct: 0, canPct: 0, totalPct: 0 }); continue; }
        const bearing = ((az / RAD) % 360 + 360) % 360, th = bearing * RAD, factor = 1 / Math.tan(alt);
        const cosN = Math.cos(th), sinE = Math.sin(th);
        // bâtiments
        bx.clearRect(0, 0, W, Hh); bx.fillStyle = "#fff";
        for (const b of blds) {
          const H = isFinite(b.h) ? b.h : Number(defH); if (!(H > 0)) continue;
          const d = H * factor, dLat = (d * cosN) / 111320, dLng = (d * sinE) / (111320 * Math.cos(b.lat * RAD));
          const hf = b.hf, m = hf.length, pts = new Array(m * 2);
          for (let i = 0; i < m; i++) { const q = hf[i]; pts[i] = q; pts[m + i] = [q[0] + dLng, q[1] + dLat]; }
          const hull = convexHull(pts); if (hull.length < 3) continue;
          bx.beginPath(); hull.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]); i ? bx.lineTo(x, y) : bx.moveTo(x, y); }); bx.closePath(); bx.fill();
        }
        const bldA = bx.getImageData(0, 0, W, Hh).data;
        // canopée
        let canA = null;
        if (canImg) {
          cx.clearRect(0, 0, W, Hh);
          const full = meanH * factor;
          for (let k = 0; k < SHAD_K; k++) {
            const frac = SHAD_K > 1 ? k / (SHAD_K - 1) : 1, d = full * frac;
            const px = ((d * sinE) / (111320 * Math.cos(cLat * RAD))) / (e - w) * W;
            const py = -((d * cosN) / 111320) / (n - s) * Hh;
            cx.drawImage(canImg, px, py, W, Hh);
          }
          canA = cx.getImageData(0, 0, W, Hh).data;
        }
        let bP = 0, cP = 0, tP = 0;
        for (let i = 3; i < zoneA.length; i += 4) { if (zoneA[i] <= 0) continue; const bb = bldA[i] > 0, cc = canA ? canA[i] > 0 : false; if (bb) bP++; if (cc) cP++; if (bb || cc) tP++; }
        out.push({ hour: hr, alt: alt / RAD, night: false, bldPct: zonePix ? 100 * bP / zonePix : 0, canPct: zonePix ? 100 * cP / zonePix : 0, totalPct: zonePix ? 100 * tP / zonePix : 0 });
      }
      const zn = scope === "import" ? (zoneRef.current?.name || "Zone importée") : scope === "roi" ? "ROI dessiné" : scope === "view" ? "Vue courante" : "Couche";
      setDashData({ data: out, meta: { zoneName: zn, date, zoneArea, dataset: canopyRef.current ? (canopyMsg?.dataset || "") : "", canopy: !!canopyRef.current, canPctStatic } });
    } finally { setDashBusy(false); }
  }, [mapRef, date, defH, trees, scope, refreshBuildings, canopyMsg]);

  const drawRoi = useCallback((map, bbox) => {
    const [w, s, e, n] = bbox;
    const fc = { type: "FeatureCollection", features: [{ type: "Feature", properties: {}, geometry: { type: "Polygon", coordinates: [[[w, s], [e, s], [e, n], [w, n], [w, s]]] } }] };
    if (!map.getSource(ROI_SRC)) map.addSource(ROI_SRC, { type: "geojson", data: fc });
    else map.getSource(ROI_SRC).setData(fc);
    if (!map.getLayer(ROI_LYR)) {
      const before = beforeId(map);
      map.addLayer({ id: ROI_LYR, type: "line", source: ROI_SRC, paint: { "line-color": "#e8590c", "line-width": 2, "line-dasharray": [2, 1] } }, before);
    }
  }, []);

  const startRoi = useCallback(() => {
    const map = mapRef?.current?.getMap?.(); if (!map) return;
    if (roiDrawing) { // annuler
      if (roiHandlerRef.current) map.off("click", roiHandlerRef.current);
      roiHandlerRef.current = null; setRoiDrawing(false); map.getCanvas().style.cursor = ""; return;
    }
    roiPtsRef.current = []; setRoiDrawing(true); map.getCanvas().style.cursor = "crosshair";
    const h = (ev) => {
      roiPtsRef.current.push([ev.lngLat.lng, ev.lngLat.lat]);
      if (roiPtsRef.current.length >= 2) {
        const [a, b] = roiPtsRef.current;
        const bbox = [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[0], b[0]), Math.max(a[1], b[1])];
        roiBboxRef.current = bbox; scopeBboxRef.current = bbox; setScope("roi"); setRoiReady(true);
        drawRoi(map, bbox);
        map.off("click", h); roiHandlerRef.current = null; map.getCanvas().style.cursor = ""; setRoiDrawing(false);
        refreshBuildings(map, bbox); compute(); if (treesRef.current) scheduleCanopy();
      }
    };
    roiHandlerRef.current = h; map.on("click", h);
  }, [mapRef, roiDrawing, drawRoi, refreshBuildings, compute, scheduleCanopy]);

  // ── Itinéraire ombragé ────────────────────────────────────────────────────
  // Échantillonneur d'ombre (bâtiments + canopée) à l'heure courante sur une bbox.
  const buildSampler = useCallback(async (bbox) => {
    const map = mapRef?.current?.getMap?.(); if (!map) return { shaded: () => false };
    const c = map.getCenter();
    const { alt, az } = sunPosition(localMs(date, Number(hour), tzOffsetHours(c.lng, tzModeRef.current)), c.lat, c.lng);
    if (alt <= 0.02) return { shaded: () => true, night: true };
    const [w, s, e, n] = bbox;
    const W = 600, Hh = Math.max(1, Math.min(1400, Math.round(W * (n - s) / (e - w))));
    const X = (lng) => (lng - w) / (e - w) * W, Y = (lat) => (n - lat) / (n - s) * Hh;
    const cv = document.createElement("canvas"); cv.width = W; cv.height = Hh;
    const ctx = cv.getContext("2d"); ctx.fillStyle = "#fff";
    const bearing = ((az / RAD) % 360 + 360) % 360, th = bearing * RAD, factor = 1 / Math.tan(alt);
    const cosN = Math.cos(th), sinE = Math.sin(th);
    for (const b of bldRef.current) {
      const H = isFinite(b.h) ? b.h : Number(defH); if (!(H > 0)) continue;
      const d = H * factor, dLat = (d * cosN) / 111320, dLng = (d * sinE) / (111320 * Math.cos(b.lat * RAD));
      const hf = b.hf, m = hf.length, pts = new Array(m * 2);
      for (let i = 0; i < m; i++) { const q = hf[i]; pts[i] = q; pts[m + i] = [q[0] + dLng, q[1] + dLat]; }
      const hull = convexHull(pts); if (hull.length < 3) continue;
      ctx.beginPath(); hull.forEach((p, i) => { const x = X(p[0]), y = Y(p[1]); i ? ctx.lineTo(x, y) : ctx.moveTo(x, y); }); ctx.closePath(); ctx.fill();
    }
    if (trees && canopyRef.current) {
      if (!canImgRef.current || canImgRef.current.url !== canopyRef.current.url) {
        const img = await loadImage(canopyRef.current.url).catch(() => null);
        canImgRef.current = { url: canopyRef.current.url, img };
      }
      const img = canImgRef.current?.img, cc = canopyRef.current.corners;
      if (img && cc) {
        const cw = cc[0][0], cn = cc[0][1], ce = cc[1][0], cs = cc[2][1];
        const dx0 = X(cw), dy0 = Y(cn), dw = (ce - cw) / (e - w) * W, dh = (cn - cs) / (n - s) * Hh;
        const full = (canopyRef.current.meanH || 0) * factor, midlat = ((s + n) / 2) * RAD;
        for (let k = 0; k < SHAD_K; k++) {
          const frac = SHAD_K > 1 ? k / (SHAD_K - 1) : 1, d = full * frac;
          const px = ((d * sinE) / (111320 * Math.cos(midlat))) / (e - w) * W, py = -((d * cosN) / 111320) / (n - s) * Hh;
          ctx.drawImage(img, dx0 + px, dy0 + py, dw, dh);
        }
      }
    }
    const data = ctx.getImageData(0, 0, W, Hh).data;
    return { shaded: (lng, lat) => { const x = Math.floor(X(lng)), y = Math.floor(Y(lat)); if (x < 0 || y < 0 || x >= W || y >= Hh) return false; return data[(y * W + x) * 4 + 3] > 10; } };
  }, [mapRef, date, hour, defH, trees]);

  const drawRoutes = useCallback((map, res) => {
    const feats = [];
    if (res.direct) feats.push({ type: "Feature", properties: { kind: "direct" }, geometry: { type: "LineString", coordinates: res.direct.coords } });
    if (res.shade) feats.push({ type: "Feature", properties: { kind: "shade" }, geometry: { type: "LineString", coordinates: res.shade.coords } });
    const sel = routeSelRef.current;
    const wExpr = ["case", ["==", ["get", "kind"], sel], 6, 3];
    const oExpr = ["case", ["==", ["get", "kind"], sel], 1, 0.5];
    if (!map.getSource(RT_SRC)) map.addSource(RT_SRC, { type: "geojson", data: { type: "FeatureCollection", features: feats } });
    else map.getSource(RT_SRC).setData({ type: "FeatureCollection", features: feats });
    if (!map.getLayer(RT_LINE)) map.addLayer({ id: RT_LINE, type: "line", source: RT_SRC,
      layout: { "line-cap": "round", "line-join": "round" },
      paint: { "line-color": ["case", ["==", ["get", "kind"], "shade"], "#2e7d4f", "#e8590c"], "line-width": wExpr, "line-opacity": oExpr } });
    else { map.setPaintProperty(RT_LINE, "line-width", wExpr); map.setPaintProperty(RT_LINE, "line-opacity", oExpr); }
    const ab = routeABRef.current || [];
    const abfc = { type: "FeatureCollection", features: ab.map((p, i) => (p ? { type: "Feature", properties: { label: i === 0 ? "A" : "B" }, geometry: { type: "Point", coordinates: p } } : null)).filter(Boolean) };
    if (!map.getSource(RT_AB)) map.addSource(RT_AB, { type: "geojson", data: abfc }); else map.getSource(RT_AB).setData(abfc);
    if (!map.getLayer(RT_AB)) map.addLayer({ id: RT_AB, type: "circle", source: RT_AB, paint: { "circle-radius": 6, "circle-color": "#111827", "circle-stroke-color": "#fff", "circle-stroke-width": 2 } });
  }, []);

  const restoreCam = useCallback((map) => {
    const pc = preCamRef.current;
    if (map && pc) { try { map.easeTo({ center: pc.center, zoom: pc.zoom, bearing: pc.bearing, pitch: pc.pitch, duration: 700 }); } catch (_) {} }
    preCamRef.current = null;
  }, []);

  const stopPreview = useCallback(() => {
    if (animRef.current?.raf) cancelAnimationFrame(animRef.current.raf);
    animRef.current = null; setPreviewing(false);
    const map = mapRef?.current?.getMap?.();
    const ms = map?.getSource?.(RT_MARK); if (ms) ms.setData({ type: "FeatureCollection", features: [] });
    if (map) restoreCam(map);
  }, [mapRef, restoreCam]);

  const startPreview = useCallback(() => {
    const map = mapRef?.current?.getMap?.(); if (!map) return;
    const g = routeGeomRef.current?.[routeSelRef.current]; if (!g) return;
    if (animRef.current?.raf) cancelAnimationFrame(animRef.current.raf);
    try { if (!map.hasImage("oma-nav-arrow")) map.addImage("oma-nav-arrow", navArrowImage(), { pixelRatio: 2 }); } catch (_) {}
    if (!map.getSource(RT_MARK)) map.addSource(RT_MARK, { type: "geojson", data: { type: "FeatureCollection", features: [] } });
    if (!map.getLayer(RT_MARK)) map.addLayer({ id: RT_MARK, type: "symbol", source: RT_MARK,
      layout: { "icon-image": "oma-nav-arrow", "icon-size": 0.7, "icon-rotate": ["get", "hdg"], "icon-rotation-alignment": "map", "icon-allow-overlap": true, "icon-ignore-placement": true } });
    if (!preCamRef.current) preCamRef.current = { center: map.getCenter().toArray(), zoom: map.getZoom(), bearing: map.getBearing(), pitch: map.getPitch() };

    const mode = navModeRef.current;                       // immersive | follow | top
    const followCam = mode === "immersive" || mode === "follow";
    const navPitch = mode === "immersive" ? 68 : mode === "follow" ? 48 : 0;
    const navZoom = mode === "immersive" ? 17.6 : mode === "follow" ? 17 : 17;

    if (!followCam) {
      // vue de dessus : cadre tout l'itinéraire une fois (caméra stable)
      let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
      for (const p of g.coords) { w = Math.min(w, p[0]); e = Math.max(e, p[0]); s = Math.min(s, p[1]); n = Math.max(n, p[1]); }
      try { map.fitBounds([[w, s], [e, n]], { padding: 70, bearing: 0, pitch: 0, duration: 700, maxZoom: 18 }); } catch (_) {}
    }
    setPreviewing(true);
    const durMs = Math.max(2000, Math.min(30000, (g.duration * 1000) / (previewSpeedRef.current * 40)));
    const start = performance.now();
    const step = (t) => {
      const f = Math.min(1, (t - start) / durMs);
      const pos = alongRoute(g.coords, g.cum, f);
      const ahead = alongRoute(g.coords, g.cum, Math.min(1, f + 0.015));
      const hdg = bearingDeg(pos, ahead);
      const ms = map.getSource(RT_MARK); if (ms) ms.setData({ type: "Feature", properties: { hdg }, geometry: { type: "Point", coordinates: pos } });
      // mode immersif/suivi : la carte tourne et suit la flèche (façon GPS 3D)
      if (followCam) { try { map.jumpTo({ center: pos, bearing: hdg, zoom: navZoom, pitch: navPitch }); } catch (_) {} }
      if (f < 1) animRef.current = { raf: requestAnimationFrame(step), start };
      else { animRef.current = null; setPreviewing(false); restoreCam(map); }
    };
    animRef.current = { raf: requestAnimationFrame(step), start };
  }, [mapRef, restoreCam]);

  const selectRoute = useCallback((kind) => {
    setRouteSel(kind); routeSelRef.current = kind; stopPreview();
    const map = mapRef?.current?.getMap?.();
    if (map && map.getLayer(RT_LINE)) {
      map.setPaintProperty(RT_LINE, "line-width", ["case", ["==", ["get", "kind"], kind], 6, 3]);
      map.setPaintProperty(RT_LINE, "line-opacity", ["case", ["==", ["get", "kind"], kind], 1, 0.5]);
    }
  }, [mapRef, stopPreview]);

  const startRouteAB = useCallback(() => {
    const map = mapRef?.current?.getMap?.(); if (!map) return;
    if (routePick) { if (routeClickRef.current) map.off("click", routeClickRef.current); routeClickRef.current = null; setRoutePick(false); map.getCanvas().style.cursor = ""; return; }
    routeABRef.current = []; setRouteAB([]); setAddr({ a: "", b: "" }); setSugg({ a: [], b: [] }); setRouteResult(null); stopPreview(); setRoutePick(true); map.getCanvas().style.cursor = "crosshair";
    const h = (ev) => {
      routeABRef.current.push([ev.lngLat.lng, ev.lngLat.lat]); setRouteAB([...routeABRef.current]);
      drawRoutes(map, {});
      if (routeABRef.current.length >= 2) { map.off("click", h); routeClickRef.current = null; map.getCanvas().style.cursor = ""; setRoutePick(false); }
    };
    routeClickRef.current = h; map.on("click", h);
  }, [mapRef, routePick, drawRoutes, stopPreview]);

  // pose/actualise A (idx 0) ou B (idx 1) — par adresse ou par clic
  const setAB = useCallback((idx, pt) => {
    const arr = (routeABRef.current || []).slice(); while (arr.length < 2) arr.push(null);
    arr[idx] = pt; routeABRef.current = arr;
    setRouteAB(arr.filter(Boolean)); setRouteResult(null); stopPreview();
    const map = mapRef?.current?.getMap?.();
    if (map) { drawRoutes(map, {}); try { map.flyTo({ center: pt, zoom: Math.max(14, map.getZoom()), duration: 600 }); } catch (_) {} }
  }, [mapRef, drawRoutes, stopPreview]);

  const onAddrChange = useCallback((which, val) => {
    setAddr((p) => ({ ...p, [which]: val }));
    clearTimeout(geoTimer.current);
    if (!val || val.trim().length < 3) { setSugg((p) => ({ ...p, [which]: [] })); return; }
    geoTimer.current = setTimeout(async () => { const res = await geocodeAddress(val).catch(() => []); setSugg((p) => ({ ...p, [which]: res })); }, 350);
  }, []);

  const pickAddr = useCallback((which, s) => {
    setAddr((p) => ({ ...p, [which]: s.label })); setSugg((p) => ({ ...p, [which]: [] }));
    setAB(which === "a" ? 0 : 1, [s.lon, s.lat]);
  }, [setAB]);

  // Repli : moteur backend (Mapbox/ORS) si le réseau des tuiles est insuffisant.
  const backendRoutes = useCallback(async (map, a, b, sampler) => {
    const rr = await fetch(`${API}/shadow/route`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ a, b, profile: "foot" }) });
    if (!rr.ok) { let m = `Erreur ${rr.status}`; try { m = (await rr.json()).detail || m; } catch (_) {} throw new Error(m); }
    const routes = (await rr.json()).routes || [];
    if (!routes.length) throw new Error("Aucun itinéraire trouvé.");
    const scored = routes.map((rt) => { const coords = rt.coordinates, dense = densify(coords, 12); let sh = 0; for (const p of dense) if (sampler.shaded(p[0], p[1])) sh++; return { coords, cum: cumDist(coords), distance: rt.distance, duration: rt.duration, shade: dense.length ? sh / dense.length : 0 }; });
    return { shade: scored.reduce((x, y) => (y.shade > x.shade ? y : x)), direct: scored.reduce((x, y) => (y.distance < x.distance ? y : x)) };
  }, []);

  const computeShadeRoutes = useCallback(async () => {
    const ab = (routeABRef.current || []).filter(Boolean);
    if (ab.length < 2) { setRouteErr("Renseignez A et B (adresse ou 2 clics sur la carte)."); return; }
    const map = mapRef?.current?.getMap?.(); if (!map) return;
    setRouteBusy(true); setRouteErr(null); setRouteResult(null); stopPreview();
    const [a, b] = ab;
    const mx = Math.max(Math.abs(a[0] - b[0]) * 0.5, 0.004), my = Math.max(Math.abs(a[1] - b[1]) * 0.5, 0.004);
    const bbox = [Math.min(a[0], b[0]) - mx, Math.min(a[1], b[1]) - my, Math.max(a[0], b[0]) + mx, Math.max(a[1], b[1]) + my];
    // cadre A→B et attend le chargement des tuiles (routes + bâtiments)
    try { map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 40, duration: 0 }); } catch (_) {}
    await new Promise((res) => { let done = false; const fin = () => { if (!done) { done = true; res(); } }; map.once("idle", fin); setTimeout(fin, 3500); });
    try {
      refreshBuildings(map, bbox);                       // bâtiments de la zone (pour l'ombre)
      const sampler = await buildSampler(bbox);
      // ── réseau piéton depuis les TUILES (aucun téléchargement) ──
      const segs = [];
      for (const f of queryTiles(map, "transportation")) {
        const cls = String(f.properties?.class || "").toLowerCase();
        if (cls.includes("motorway") || cls.includes("trunk") || cls === "raceway") continue;
        const g = f.geometry; if (!g) continue;
        if (g.type === "LineString") segs.push(g.coordinates);
        else if (g.type === "MultiLineString") for (const l of g.coordinates) segs.push(l);
      }
      let res = null, viaGraph = false, note = "";
      if (segs.length >= 4) {
        const graph = buildGraph(segs, sampler);
        const snap = (p) => { let bk = null, bd = Infinity; for (const [k, q] of graph.nodes) { const dd = haversine(p, q); if (dd < bd) { bd = dd; bk = k; } } return bk; };
        const ka = snap(a), kb = snap(b);
        const mkRoute = (path) => { if (!path || path.length < 1) return null; const coords = [a, ...path, b]; const cum = cumDist(coords); const dist = cum[cum.length - 1]; const dense = densify(coords, 12); let sh = 0; for (const p of dense) if (sampler.shaded(p[0], p[1])) sh++; return { coords, cum, distance: dist, duration: dist / 1.35, shade: dense.length ? sh / dense.length : 0 }; };
        const samePath = (p, q) => !!p && !!q && p.length === q.length && p.every((pt, i) => pt[0] === q[i][0] && pt[1] === q[i][1]);
        const pDirect = dijkstra(graph, ka, kb, (e) => e.len);
        // pénalité croissante : force un détour ombragé s'il en existe un
        let pShade = null;
        for (const K of [3, 6, 12]) {
          pShade = dijkstra(graph, ka, kb, (e) => e.len * (1 + K * (1 - e.shade)));
          if (!samePath(pShade, pDirect)) break;
        }
        const direct = mkRoute(pDirect), shade = mkRoute(pShade);
        if (direct && shade) { res = { shade, direct }; viaGraph = true; }
      }
      if (!res) { res = await backendRoutes(map, a, b, sampler); note = "Réseau des tuiles insuffisant ici — itinéraire du moteur (sans optimisation d'ombre) ; zoomez sur la zone pour l'optimisation locale."; }
      const same = res.shade === res.direct || (Math.abs(res.shade.distance - res.direct.distance) < 2 && Math.abs(res.shade.shade - res.direct.shade) < 0.01);
      routeGeomRef.current = res;
      setRouteResult({ ...res, night: sampler.night, same, graph: viaGraph });
      if (note) setRouteErr(note);
      drawRoutes(map, res);
    } catch (e) {
      // dernier recours : backend seul
      try { const sampler = await buildSampler(bbox); const res = await backendRoutes(map, a, b, sampler); routeGeomRef.current = res; setRouteResult({ ...res, night: sampler.night, same: res.shade === res.direct, graph: false }); drawRoutes(map, res); setRouteErr("Optimisation locale impossible — itinéraire du moteur. " + (e.message || "")); }
      catch (e2) { setRouteErr(e.message || String(e)); }
    } finally { setRouteBusy(false); }
  }, [mapRef, buildSampler, drawRoutes, stopPreview, refreshBuildings, backendRoutes]);

  useEffect(() => {
    const map = mapRef?.current?.getMap?.();
    if (!map) return;
    const onMove = () => { refreshBuildings(map, scopeBboxRef.current); compute(); refreshSun(); if (treesRef.current) scheduleCanopy(); };
    const onStyle = () => { setTimeout(() => { refreshBuildings(map, scopeBboxRef.current); compute(); if (treesRef.current) scheduleCanopy(); }, 300); };
    map.on("moveend", onMove); map.on("styledata", onStyle);
    return () => { map.off("moveend", onMove); map.off("styledata", onStyle); };
  }, [mapRef, refreshBuildings, compute, scheduleCanopy, refreshSun]);

  useEffect(() => {
    if (!playing) { if (playRef.current) { clearInterval(playRef.current); playRef.current = null; } return; }
    // balaie du LEVER au COUCHER du soleil (lieu + jour). Pas fin + cadence élevée
    // → fluide (enveloppe convexe précalculée + filtre d'emprise = images légères).
    const lo0 = sunRef.current?.riseH ?? 6, hi0 = sunRef.current?.setH ?? 21;
    setHour((h) => (h < lo0 || h > hi0 ? lo0 : h));
    playRef.current = setInterval(() => {
      setHour((h) => { const lo = sunRef.current?.riseH ?? 6, hi = sunRef.current?.setH ?? 21; const nx = h + 0.12; return nx > hi ? lo : nx; });
    }, 80);
    return () => { if (playRef.current) { clearInterval(playRef.current); playRef.current = null; } };
  }, [playing]);

  useEffect(() => {
    return () => {
      const map = mapRef?.current?.getMap?.();
      if (playRef.current) clearInterval(playRef.current);
      if (animRef.current?.raf) cancelAnimationFrame(animRef.current.raf);
      if (map && preCamRef.current) { try { map.jumpTo({ bearing: preCamRef.current.bearing }); } catch (_) {} }
      clearTimeout(canopyTimer.current); clearTimeout(geoTimer.current);
      if (map && roiHandlerRef.current) { try { map.off("click", roiHandlerRef.current); } catch (_) {} }
      if (map && routeClickRef.current) { try { map.off("click", routeClickRef.current); } catch (_) {} }
      if (map) { try { map.getCanvas().style.cursor = ""; } catch (_) {} }
      try {
        if (map) {
          const ids = [LYR, IMG_DISP, ROI_LYR, ZONE_FILL, ZONE_LINE, RT_LINE, RT_AB, RT_MARK, ...Array.from({ length: SHAD_K }, (_, i) => shadId(i))];
          ids.forEach((id) => { if (map.getLayer(id)) map.removeLayer(id); });
          const srcs = [SRC, IMG_DISP, ROI_SRC, ZONE_SRC, RT_SRC, RT_AB, RT_MARK, ...Array.from({ length: SHAD_K }, (_, i) => shadId(i))];
          srcs.forEach((id) => { if (map.getSource(id)) map.removeSource(id); });
        }
      } catch (_) {}
    };
  }, [mapRef]);

  const hh = Math.floor(hour), mm = Math.round((hour - hh) * 60);
  const clock = `${String(hh).padStart(2, "0")}:${String(mm).padStart(2, "0")}`;
  const statsLocked = trees && (canopyMsg?.busy || !canopyRef.current);

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 12, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", boxSizing: "border-box" };
  const Code = ({ children }) => (<code style={{ fontFamily: M, fontSize: 11.5, background: C.acc + "12", color: C.acc, padding: "1px 5px", borderRadius: 4 }}>{children}</code>);
  const tabBtn = (id, label) => (
    <button key={id} onClick={() => setTab(id)}
      style={{ fontFamily: F, fontSize: 12, fontWeight: tab === id ? 600 : 500, padding: "5px 10px", cursor: "pointer",
        background: "transparent", color: tab === id ? C.acc : C.mut, border: "none",
        borderBottom: `2px solid ${tab === id ? C.acc : "transparent"}`, marginBottom: -1 }}>{label}</button>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${C.bdr}` }}>
        {tabBtn("sim", "Ombrage")}
        {tabBtn("route", "Itinéraire")}
        {tabBtn("def", "Définition")}
      </div>

      {tab === "def" ? (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 12, fontFamily: F, fontSize: 12.5, lineHeight: 1.55, color: C.txt, paddingRight: 4 }}>
          <p style={{ margin: 0 }}>Simule l'<b>ombre portée au sol</b> des <b>bâtiments</b> et de la <b>canopée</b>, à une date et une heure, et la fait défiler sur la journée.</p>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Bâtiments — sans téléchargement</div>
            <p style={{ margin: 0, color: C.mut }}>Lus des <b>tuiles</b> (couche <Code>building</Code>, <Code>render_height</Code>). Ombre = <i>H / tan(soleil)</i>, direction opposée au soleil (SunCalc). Sol plat.</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Canopée — Meta ~1 m</div>
            <p style={{ margin: 0, color: C.mut }}>Modèle <b>WRI/Meta 2020</b> (Earth Engine) en aperçu raster lissé (vraie emprise, vert) ; son ombre = plusieurs copies sombres empilées de la base au décalage plein (sans trou).</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Emprise & statistiques</div>
            <p style={{ margin: 0, color: C.mut }}>Calcul sur la vue, l'emprise d'une couche, ou un <b>ROI</b> dessiné (2 clics). Le bouton <b>Statistiques</b> (actif une fois la canopée chargée) donne les surfaces ombragées de la zone.</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Itinéraire ombragé</div>
            <p style={{ margin: 0, color: C.mut }}>Onglet <b>Itinéraire</b> : deux trajets piétons A → B — <b>plus ombragé</b> ou <b>plus direct</b> — évalués selon l'ombre à l'heure choisie, avec prévisualisation animée (accélérée).</p>
          </div>
        </div>
      ) : tab === "route" ? (
        <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1, minHeight: 0, overflowY: "auto", paddingRight: 4 }}>
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
Itinéraires piétons A → B <b>optimisés sur le réseau des tuiles</b> (Dijkstra pondéré par l'ombre, 100 % local) : <b>plus ombragé</b> vs <b>plus direct</b>, à la date/heure ci-dessous. Prévisualisez le parcours (vue immersive ou de dessus).
          </div>

          <div style={{ display: "flex", gap: 8, alignItems: "flex-end" }}>
            <div style={{ flex: 1 }}>
              <div style={lbl}>Date de l'ombre</div>
              <input type="date" value={date} onChange={(e) => setDate(e.target.value)} style={{ ...inp, width: "100%" }} />
            </div>
            <div style={{ width: 150 }}>
              <div style={lbl}>Heure · {clock}</div>
              <input type="range" min={0} max={24} step={0.25} value={hour} onChange={(e) => { setPlaying(false); setHour(Number(e.target.value)); }} style={{ width: "100%" }} />
            </div>
          </div>
          <div style={{ fontFamily: F, fontSize: 10, color: C.dim, marginTop: -4 }}>
            {sunTimes && sunTimes.off != null ? fmtOffset(sunTimes.off) : "heure locale"} · fuseau réglable dans l'onglet Ombrage.
          </div>

          {[["a", "Départ (A)"], ["b", "Arrivée (B)"]].map(([which, label]) => (
            <div key={which} style={{ position: "relative" }}>
              <div style={lbl}>{label}</div>
              <input value={addr[which]} onChange={(e) => onAddrChange(which, e.target.value)} placeholder="Adresse ou lieu…" style={{ ...inp, width: "100%" }} />
              {sugg[which].length > 0 && (
                <div style={{ position: "absolute", top: "100%", left: 0, right: 0, zIndex: 5, background: C.card || C.bg, border: `0.5px solid ${C.bdr}`, borderRadius: 7, marginTop: 2, maxHeight: 170, overflowY: "auto", boxShadow: "0 8px 22px rgba(0,0,0,0.18)" }}>
                  {sugg[which].map((s, i) => (
                    <div key={i} onClick={() => pickAddr(which, s)}
                      style={{ fontFamily: F, fontSize: 11, padding: "6px 9px", cursor: "pointer", color: C.txt, borderBottom: i < sugg[which].length - 1 ? `0.5px solid ${C.bdr}` : "none" }}>
                      {s.label}
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
          <div style={{ fontFamily: F, fontSize: 10, color: C.dim, textAlign: "center" }}>— ou placez A/B sur la carte —</div>

          <button onClick={startRouteAB}
            style={{ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "8px 12px", cursor: "pointer",
              background: routePick ? C.acc : "transparent", color: routePick ? "#fff" : C.acc, border: `1px solid ${C.acc}66`, borderRadius: 8 }}>
            {routePick ? `Cliquez ${routeAB.length === 0 ? "le point A" : "le point B"} sur la carte…` : (routeAB.length >= 2 ? "Redéfinir A → B" : "Définir A → B (2 clics)")}
          </button>

          <button onClick={computeShadeRoutes} disabled={routeBusy || routeAB.length < 2}
            style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "9px 14px", cursor: (routeBusy || routeAB.length < 2) ? "not-allowed" : "pointer",
              background: (routeBusy || routeAB.length < 2) ? C.bg2 || C.bg : C.acc, color: (routeBusy || routeAB.length < 2) ? C.dim : "#fff", border: `1px solid ${routeAB.length < 2 ? C.bdr : C.acc}`, borderRadius: 8 }}>
            {routeBusy ? "Calcul…" : "Calculer les itinéraires"}
          </button>

          {routeErr && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{routeErr}</div>}

          {routeResult && (
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {routeResult.night && <div style={{ fontFamily: F, fontSize: 11, color: C.dim }}>🌙 Nuit à cette heure — tout est « à l'ombre ». Choisissez une heure de jour pour comparer.</div>}
              {[["shade", "🌳 Plus ombragé", "#2e7d4f", routeResult.shade], ["direct", "➡ Plus direct", "#e8590c", routeResult.direct]].map(([kind, label, col, r]) => (
                <button key={kind} onClick={() => selectRoute(kind)}
                  style={{ textAlign: "left", fontFamily: F, cursor: "pointer", padding: "9px 11px", borderRadius: 8,
                    border: `1.5px solid ${routeSel === kind ? col : C.bdr}`, background: routeSel === kind ? col + "12" : (C.bg2 || C.bg) }}>
                  <div style={{ fontSize: 12.5, fontWeight: 700, color: col }}>{label}</div>
                  <div style={{ fontSize: 11.5, color: C.txt, marginTop: 2 }}>
                    <b>{Math.round(r.shade * 100)}%</b> à l'ombre · {(r.distance / 1000).toFixed(2)} km · {Math.round(r.duration / 60)} min
                  </div>
                </button>
              ))}
              {routeResult.same && <div style={{ fontFamily: F, fontSize: 10, color: C.dim }}>Le plus ombragé = le plus direct ici. {routeResult.night ? "Il fait nuit." : "À cette heure les ombres sont peut-être courtes — essayez une heure de soleil plus rasant (matin/fin d'après-midi)."}</div>}
              <div style={{ fontFamily: F, fontSize: 10, color: C.dim }}>{routeResult.graph ? "✓ Optimisé sur le réseau local (tuiles) pondéré par l'ombre." : "⚠ Réseau local insuffisant → itinéraire du moteur (non optimisé)."}</div>
            </div>
          )}

          {routeResult && (
            <div style={{ borderTop: `0.5px solid ${C.bdr}`, paddingTop: 10, display: "flex", flexDirection: "column", gap: 6 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                <button onClick={() => (previewing ? stopPreview() : startPreview())}
                  style={{ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "6px 12px", cursor: "pointer",
                    background: previewing ? C.acc : "transparent", color: previewing ? "#fff" : C.acc, border: `1px solid ${C.acc}66`, borderRadius: 7 }}>
                  {previewing ? "❚❚ Stop" : "▶ Prévisualiser"}
                </button>
                <span style={{ fontFamily: F, fontSize: 10.5, color: C.dim }}>Vitesse</span>
                {[1, 2, 4, 8].map((sp) => (
                  <button key={sp} onClick={() => { setPreviewSpeed(sp); previewSpeedRef.current = sp; if (previewing) { if (animRef.current?.raf) cancelAnimationFrame(animRef.current.raf); startPreview(); } }}
                    style={{ fontFamily: M, fontSize: 11, padding: "3px 8px", cursor: "pointer", borderRadius: 6,
                      border: `1px solid ${previewSpeed === sp ? C.acc : C.bdr}`, background: previewSpeed === sp ? C.acc + "18" : "transparent", color: previewSpeed === sp ? C.acc : C.mut }}>
                    ×{sp}
                  </button>
                ))}
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap" }}>
                <span style={{ fontFamily: F, fontSize: 10.5, color: C.dim }}>Vue</span>
                {[["immersive", "Immersive 3D"], ["follow", "Suivi"], ["top", "De dessus"]].map(([m, label]) => (
                  <button key={m} onClick={() => { setNavMode(m); navModeRef.current = m; if (previewing) { if (animRef.current?.raf) cancelAnimationFrame(animRef.current.raf); startPreview(); } }}
                    style={{ fontFamily: F, fontSize: 11, padding: "3px 9px", cursor: "pointer", borderRadius: 6,
                      border: `1px solid ${navMode === m ? C.acc : C.bdr}`, background: navMode === m ? C.acc + "18" : "transparent", color: navMode === m ? C.acc : C.mut }}>
                    {label}
                  </button>
                ))}
              </div>
              <div style={{ fontFamily: F, fontSize: 10, color: C.dim }}>Immersive 3D / Suivi : la carte tourne et suit la flèche (façon GPS). De dessus : vue d'ensemble stable. La vue est restaurée à la fin.</div>
            </div>
          )}
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 12, flex: 1, minHeight: 0, overflowY: "auto", paddingRight: 4 }}>
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
            Ombre des <b>bâtiments</b> + <b>canopée réelle</b> (Meta). Fond <b>Liberty</b>, <b>3D</b> et zoom réglés automatiquement — déplacez-vous sur une ville.
          </div>

          <div>
            <div style={lbl}>Emprise du calcul</div>
            <div style={{ display: "flex", gap: 6 }}>
              <select value={scope} onChange={(e) => changeScope(e.target.value)} style={{ ...inp, flex: 1 }}>
                <option value="view">Vue courante de la carte</option>
                {zoneName && <option value="import">Zone importée : {zoneName}</option>}
                {roiReady && <option value="roi">ROI dessiné</option>}
                {layerOptions.map((o) => <option key={o.id} value={o.id}>Couche : {o.name}</option>)}
              </select>
              <button onClick={startRoi}
                style={{ fontFamily: F, fontSize: 11, fontWeight: 500, padding: "0 10px", cursor: "pointer",
                  background: roiDrawing ? C.acc : "transparent", color: roiDrawing ? "#fff" : C.acc, border: `1px solid ${C.acc}66`, borderRadius: 7, whiteSpace: "nowrap" }}>
                {roiDrawing ? "Annuler" : "ROI"}
              </button>
            </div>
            <input ref={fileRef} type="file" accept=".geojson,.json" style={{ display: "none" }}
              onChange={(e) => { const f = e.target.files?.[0]; e.target.value = ""; onImportZone(f); }} />
            <button onClick={() => fileRef.current?.click()}
              style={{ fontFamily: F, fontSize: 11, fontWeight: 500, padding: "6px 10px", marginTop: 6, width: "100%", cursor: "pointer",
                background: "transparent", color: C.acc, border: `1px dashed ${C.acc}66`, borderRadius: 7 }}>
              ⭱ Importer une zone (GeoJSON)
            </button>
            {roiDrawing && <div style={{ fontFamily: F, fontSize: 10.5, color: C.acc, marginTop: 3 }}>Cliquez 2 coins sur la carte pour définir le ROI.</div>}
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <div style={{ flex: 1 }}>
              <div style={lbl}>Date</div>
              <input type="date" value={date} onChange={(e) => setDate(e.target.value)} style={{ ...inp, width: "100%" }} />
            </div>
            <div style={{ width: 96 }}>
              <div style={lbl}>Heure</div>
              <div style={{ fontFamily: M, fontSize: 15, fontWeight: 600, color: C.txt, padding: "5px 0", textAlign: "center" }}>{clock}</div>
            </div>
          </div>

          <div>
            <div style={lbl}>Fuseau horaire</div>
            <select value={tzMode} onChange={(e) => setTzMode(e.target.value)} style={{ ...inp, width: "100%" }}>
              <option value="auto">Heure locale du lieu (longitude){sunTimes && sunTimes.off != null ? ` · ${fmtOffset(sunTimes.off)}` : ""}</option>
              <option value="utc">UTC</option>
              <option value="browser">Fuseau du navigateur</option>
            </select>
            <div style={{ fontFamily: F, fontSize: 10, color: C.dim, marginTop: 3 }}>« Longitude » = heure solaire moyenne du lieu (correcte p.ex. aux US, ~1 h d'écart en Europe à cause du fuseau politique).</div>
          </div>

          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <button onClick={() => setPlaying((p) => !p)}
                style={{ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "5px 12px", cursor: "pointer",
                  background: playing ? C.acc : "transparent", color: playing ? "#fff" : C.acc, border: `1px solid ${C.acc}66`, borderRadius: 7, flexShrink: 0 }}>
                {playing ? "❚❚ Pause" : "▶ Journée"}
              </button>
              <input type="range" min={0} max={24} step={0.25} value={hour}
                onChange={(e) => { setPlaying(false); setHour(Number(e.target.value)); }} style={{ flex: 1 }} />
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", fontFamily: M, fontSize: 9, color: C.dim, marginTop: 2 }}>
              <span>0h</span><span>6h</span><span>12h</span><span>18h</span><span>24h</span>
            </div>
          </div>

          <div style={{ background: C.bg2 || C.bg, border: `0.5px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", fontFamily: F, fontSize: 11.5, color: C.txt }}>
            {!info ? "Calcul…" : info.night ? (
              <span>🌙 Soleil sous l'horizon ({info.alt.toFixed(0)}°) — nuit, pas d'ombre.</span>
            ) : (
              <span>☀️ Soleil à <b>{info.alt.toFixed(0)}°</b> · ombre ≈ <b>{info.factor.toFixed(1)}×</b> la hauteur · <b>{info.count}</b> bâtiment(s).</span>
            )}
            {sunTimes && (sunTimes.polar
              ? <div style={{ marginTop: 4, color: C.dim }}>☀️ Jour/nuit polaire ce jour-là (pas de lever/coucher).</div>
              : <div style={{ marginTop: 4, color: C.mut }}>🌅 Lever <b>{sunTimes.riseStr}</b> · 🌇 Coucher <b>{sunTimes.setStr}</b> <span style={{ color: C.dim }}>({sunTimes.off != null ? fmtOffset(sunTimes.off) : "heure locale"})</span></div>)}
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <div style={{ flex: 1 }}>
              <div style={lbl}>Opacité ombre · {Math.round(opacity * 100)}%</div>
              <input type="range" min={0.1} max={0.8} step={0.05} value={opacity} onChange={(e) => setOpacity(Number(e.target.value))} style={{ width: "100%" }} />
            </div>
            <div style={{ width: 110 }}>
              <div style={lbl}>Haut. défaut · m</div>
              <input type="number" min={2} max={200} value={defH} onChange={(e) => setDefH(Number(e.target.value))} style={{ ...inp, width: "100%" }} />
            </div>
          </div>

          <div style={{ borderTop: `0.5px solid ${C.bdr}`, paddingTop: 10 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 7, fontFamily: F, fontSize: 11.5, color: C.txt, cursor: "pointer" }}>
              <input type="checkbox" checked={trees} onChange={(e) => setTrees(e.target.checked)} />
              🌳 Canopée <span style={{ color: C.dim }}>(Meta ~1 m, vraie emprise)</span>
            </label>
            {trees && (
              <div style={{ fontFamily: F, fontSize: 10.5, marginTop: 4 }}>
                {canopyMsg?.busy ? <span style={{ color: C.mut }}>⏳ Chargement de la canopée Meta…</span>
                  : canopyMsg?.ok ? <span style={{ color: "#2e7d4f" }}>Canopée {String(canopyMsg.dataset || "").includes("Meta") ? "Meta ~1 m" : "ETH 10 m"} affichée{canopyMsg.meanH ? ` · h. moy. ${canopyMsg.meanH} m` : ""}.</span>
                  : canopyMsg?.err ? <span style={{ color: C.dim }}>Canopée indisponible — {canopyMsg.err}</span>
                  : <span style={{ color: C.dim }}>Vraie emprise des arbres (raster) + son ombre.</span>}
              </div>
            )}
          </div>

          <div style={{ borderTop: `0.5px solid ${C.bdr}`, paddingTop: 10 }}>
            <button onClick={computeDaily} disabled={statsLocked || dashBusy}
              style={{ fontFamily: F, fontSize: 12.5, fontWeight: 600, padding: "9px 14px", width: "100%",
                cursor: (statsLocked || dashBusy) ? "not-allowed" : "pointer",
                background: (statsLocked || dashBusy) ? C.bg2 || C.bg : C.acc, color: (statsLocked || dashBusy) ? C.dim : "#fff",
                border: `1px solid ${statsLocked ? C.bdr : C.acc}`, borderRadius: 8 }}>
              {dashBusy ? "Calcul de la journée…" : statsLocked ? "Tableau de bord (attente canopée…)" : "📊 Tableau de bord — ombrage sur la journée"}
            </button>
            <div style={{ fontFamily: F, fontSize: 10, color: C.dim, marginTop: 4 }}>
              Ouvre un tableau de bord (graphiques + tableau) : % de la zone à l'ombre heure par heure, sur l'emprise choisie.
            </div>
          </div>
        </div>
      )}

      {dashData && <ShadowDashboard data={dashData.data} meta={dashData.meta} onClose={() => setDashData(null)} />}
    </div>
  );
}
