/**
 * Scene3DPanel.jsx — Vue Globe + données 3D (deck.gl / Gaussian splats)
 *
 *  - Bascule projection Globe ⇆ Mercator (MapLibre v5)
 *  - Ajout de couches 3D superposées à la carte :
 *      · 3D Tiles (tileset.json)        géoréférencé
 *      · glTF / GLB (modèle)            placé sur lon/lat
 *      · Nuage de points LAS / LAZ      placé sur lon/lat (offsets mètres)
 *  - Gaussian splats (.ply/.splat/.ksplat) → visualiseur plein écran autonome
 *
 *  Les couches 3D persistent sur la carte même panneau fermé (gestionnaire
 *  singleton deck3d).  Nécessite `npm install` (deck.gl, loaders.gl, three…).
 */
import { useState, useEffect, useCallback, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import {
  add3DLayer, add3DPointCloudData, remove3DLayer, set3DVisible, set3DOpacity,
  list3DLayers, setGlobe, isGlobe,
} from "../utils/deck3d";
import SplatViewerModal from "./SplatViewerModal";
import {
  IcBuilding, IcCube, IcCircleDot, IcSparkles, IcUpload, IcGlobe, IcMapPin,
  IcPlus, IcEye, IcEyeOff, IcX, IcInfo,
} from "../icons";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// Upload LiDAR avec progression (XHR) → distingue envoi vs conversion serveur
function uploadLidar(file, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${API}/api/lidar/points`);
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) onProgress(e.loaded / e.total);
    };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try { resolve(JSON.parse(xhr.responseText)); }
        catch (_) { reject(new Error("Réponse serveur illisible.")); }
      } else if (xhr.status === 413) {
        reject(new Error(`Fichier trop volumineux (${(file.size / 1048576).toFixed(0)} Mo) : limite d'upload du serveur (nginx « client_max_body_size »). Augmentez-la (ex. 1024M) puis rechargez nginx.`));
      } else {
        let msg = `Erreur ${xhr.status}`;
        try { msg = JSON.parse(xhr.responseText).detail || msg; } catch (_) {}
        reject(new Error(msg));
      }
    };
    xhr.onerror = () => reject(new Error("Échec réseau pendant l'upload."));
    xhr.ontimeout = () => reject(new Error("Délai d'upload dépassé."));
    const fd = new FormData();
    fd.append("file", file);
    xhr.send(fd);
  });
}

// Réduit un .las NON compressé côté navigateur : copie l'en-tête + les VLRs
// (donc le CRS) + une fraction des points, et renvoie un petit fichier .las.
// → on n'upload que ~5 Mo au lieu de 177 Mo. Renvoie null si LAZ/compressé,
// déjà petit, ou trop gros pour la mémoire navigateur (→ upload normal).
async function buildSmallLas(file, target) {
  const headBuf = await file.slice(0, 400).arrayBuffer();
  const dv = new DataView(headBuf);
  if (dv.getUint8(0) !== 0x4c || dv.getUint8(1) !== 0x41 || dv.getUint8(2) !== 0x53 || dv.getUint8(3) !== 0x46)
    return null;                                   // pas "LASF"
  const verMinor = dv.getUint8(25);
  const offsetPts = dv.getUint32(96, true);
  const pfRaw = dv.getUint8(104);
  if (pfRaw & 0x80) return null;                   // LAZ compressé → serveur
  const recLen = dv.getUint16(105, true);
  let count = dv.getUint32(107, true);
  if (verMinor >= 4) {
    const lo = dv.getUint32(247, true), hi = dv.getUint32(251, true);
    const real = hi * 4294967296 + lo;
    if (real > 0) count = real;
  }
  if (!count || !recLen || !offsetPts) return null;
  if (count <= target) return null;                // déjà petit → upload direct
  const ptBytes = count * recLen;
  if (ptBytes > 1_500_000_000) return null;        // trop gros pour le navigateur

  const pre = new Uint8Array(await file.slice(0, offsetPts).arrayBuffer());
  const pts = new Uint8Array(await file.slice(offsetPts, offsetPts + ptBytes).arrayBuffer());

  const stride = Math.max(1, Math.floor(count / target));
  const m = Math.floor(count / stride);
  const out = new Uint8Array(pre.length + m * recLen);
  out.set(pre, 0);
  let w = pre.length;
  for (let j = 0; j < m; j++) {
    const src = (j * stride) * recLen;
    out.set(pts.subarray(src, src + recLen), w);
    w += recLen;
  }
  const odv = new DataView(out.buffer);
  odv.setUint32(107, m, true);                     // legacy point count
  for (let k = 0; k < 5; k++) odv.setUint32(111 + k * 4, 0, true);  // by-return legacy = 0
  if (verMinor >= 4) { odv.setUint32(247, m, true); odv.setUint32(251, 0, true); }

  return new File([out], file.name.replace(/\.las$/i, "_sub.las"), { type: "application/octet-stream" });
}

// base64 → ArrayBuffer (pour les nuages convertis par le backend)
function _b64ToBuf(b64) {
  const bin = atob(b64);
  const len = bin.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) bytes[i] = bin.charCodeAt(i);
  return bytes.buffer;
}

// ── Classes ASPRS (classification LiDAR standard) ─────────────────────────
const ASPRS_CLASSES = {
  0: ["Jamais classé", "#bdbdbd"], 1: ["Non classé", "#9e9e9e"], 2: ["Sol", "#8c6d31"],
  3: ["Végétation basse", "#c2e699"], 4: ["Végétation moy.", "#78c679"], 5: ["Végétation haute", "#238443"],
  6: ["Bâtiment", "#e31a1c"], 7: ["Bruit bas", "#fb9a99"], 8: ["Clé modèle", "#dd3497"], 9: ["Eau", "#1f78b4"],
  10: ["Voie ferrée", "#6a3d9a"], 11: ["Route", "#525252"], 12: ["Couvert", "#969696"],
  13: ["Câble (garde)", "#ff7f00"], 14: ["Câble (cond.)", "#ffbf00"], 15: ["Pylône", "#b15928"],
  16: ["Connecteur", "#fdbf6f"], 17: ["Pont", "#cab2d6"], 18: ["Bruit haut", "#fb9a99"],
};
const CLASS_FALLBACK = "#bdbdbd";

function hexToRgb(hex) {
  const h = hex.replace("#", "");
  return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)];
}
// Table de correspondance classe(0-255) → [r,g,b]
const CLASS_RGB = Array.from({ length: 256 }, (_, i) => hexToRgb(ASPRS_CLASSES[i]?.[1] || CLASS_FALLBACK));

// Construit une table classe(0-255) → [r,g,b] à partir des couleurs ASPRS par
// défaut, écrasée par les couleurs choisies par l'utilisateur (overrides hex).
function buildClassTable(overrides) {
  if (!overrides || Object.keys(overrides).length === 0) return CLASS_RGB;
  const t = CLASS_RGB.map(c => c.slice());
  for (const [k, hex] of Object.entries(overrides)) {
    const i = Number(k);
    if (i >= 0 && i < 256 && hex) t[i] = hexToRgb(hex);
  }
  return t;
}

// Couleurs Uint8 [r,g,b,...] depuis un tableau de classifications
function classColors(clsArr, table) {
  const t = table || CLASS_RGB;
  const n = clsArr.length;
  const out = new Uint8Array(n * 3);
  for (let i = 0; i < n; i++) {
    const rgb = t[clsArr[i]] || t[0] || CLASS_RGB[0];
    out[i * 3] = rgb[0]; out[i * 3 + 1] = rgb[1]; out[i * 3 + 2] = rgb[2];
  }
  return out;
}

// Rampe de couleur type viridis (t ∈ [0,1]) → [r,g,b]
const _RAMP = [[68, 1, 84], [59, 82, 139], [33, 145, 140], [94, 201, 98], [253, 231, 37]];
function rampColor(t) {
  t = t < 0 ? 0 : t > 1 ? 1 : t;
  const seg = t * (_RAMP.length - 1);
  const i = Math.floor(seg), f = seg - i;
  const a = _RAMP[i], b = _RAMP[Math.min(i + 1, _RAMP.length - 1)];
  return [Math.round(a[0] + (b[0] - a[0]) * f),
          Math.round(a[1] + (b[1] - a[1]) * f),
          Math.round(a[2] + (b[2] - a[2]) * f)];
}

// Couleurs Uint8 par altitude Z (offsets dans positions[i*3+2])
function elevationColors(positions, count) {
  let zmin = Infinity, zmax = -Infinity;
  for (let i = 0; i < count; i++) { const z = positions[i * 3 + 2]; if (z < zmin) zmin = z; if (z > zmax) zmax = z; }
  const span = (zmax - zmin) || 1;
  const out = new Uint8Array(count * 3);
  for (let i = 0; i < count; i++) {
    const [r, g, b] = rampColor((positions[i * 3 + 2] - zmin) / span);
    out[i * 3] = r; out[i * 3 + 1] = g; out[i * 3 + 2] = b;
  }
  return out;
}

// Ne conserve que les points dont la classification ∈ selSet (filtre par classe).
// selSet = Set de numéros de classe visibles. Renvoie un nuage réduit.
function filterByClass(full, selSet) {
  const { positions, rgb, classification, count } = full;
  const idx = new Int32Array(count);
  let k = 0;
  for (let i = 0; i < count; i++) if (selSet.has(classification[i])) idx[k++] = i;
  const pos = new Float32Array(k * 3);
  const col = rgb ? new Uint8Array(k * 3) : null;
  const cls = new Uint8Array(k);
  for (let j = 0; j < k; j++) {
    const i = idx[j];
    pos[j * 3] = positions[i * 3]; pos[j * 3 + 1] = positions[i * 3 + 1]; pos[j * 3 + 2] = positions[i * 3 + 2];
    if (col) { col[j * 3] = rgb[i * 3]; col[j * 3 + 1] = rgb[i * 3 + 1]; col[j * 3 + 2] = rgb[i * 3 + 2]; }
    cls[j] = classification[i];
  }
  return { positions: pos, rgb: col, classification: cls, count: k };
}

// Sous-échantillonnage régulier (stride) d'un nuage pour l'affichage dynamique
function subsamplePC(full, target) {
  const { positions, rgb, classification, count } = full;
  if (target >= count) return { positions, rgb, classification, count };
  const step = Math.ceil(count / target);
  const n = Math.ceil(count / step);
  const pos = new Float32Array(n * 3);
  const col = rgb ? new Uint8Array(n * 3) : null;
  const cls = classification ? new Uint8Array(n) : null;
  let j = 0;
  for (let i = 0; i < count && j < n; i += step, j++) {
    pos[j * 3] = positions[i * 3]; pos[j * 3 + 1] = positions[i * 3 + 1]; pos[j * 3 + 2] = positions[i * 3 + 2];
    if (col) { col[j * 3] = rgb[i * 3]; col[j * 3 + 1] = rgb[i * 3 + 1]; col[j * 3 + 2] = rgb[i * 3 + 2]; }
    if (cls) { cls[j] = classification[i]; }
  }
  return { positions: pos, rgb: col, classification: cls, count: j };
}

const KINDS = [
  { id: "3dtiles",    label: "3D Tiles",        icon: IcBuilding,  hint: "URL d'un tileset.json (géoréférencé)", placed: false },
  { id: "model",      label: "glTF / GLB",      icon: IcCube,      hint: "Modèle 3D placé sur un point",         placed: true  },
  { id: "pointcloud", label: "Nuage LAS/LAZ",   icon: IcCircleDot, hint: "Nuage de points placé sur un point",   placed: true  },
  { id: "splat",      label: "Gaussian splat",  icon: IcSparkles,  hint: "Visualiseur plein écran (.ply/.splat/.ksplat)", placed: false },
];

const KIND_ICON = Object.fromEntries(KINDS.map(k => [k.id, k.icon]));

// Endpoint des Google Photorealistic 3D Tiles (couverture mondiale).
const GOOGLE_3DTILES_URL = "https://tile.googleapis.com/v1/3dtiles/root.json";

// Services 3D Tiles gratuits, sans clé API (standards ouverts, mondiaux).
const TILES3D_PRESETS = [
  { label: "Re:Earth Buildings — bâtiments mondiaux, sans clé", url: "https://buildings.reearth.land/tileset.json" },
];

// Retire un éventuel paramètre ?key=… d'une URL (on préfère l'en-tête).
function stripKeyParam(u) {
  try { const x = new URL(u); x.searchParams.delete("key"); return x.toString(); }
  catch (_) { return u; }
}

// Vérifie qu'un tileset 3D Tiles se charge AVANT de créer la couche deck.gl :
// transforme le 403 silencieux (rejet promise console) en message actionnable.
// Renvoie null si OK, sinon { status, detail }.
async function preflight3DTiles(url, apiKey) {
  const opts = apiKey ? { headers: { "X-GOOG-API-KEY": apiKey } } : {};
  let resp;
  try {
    resp = await fetch(url, opts);
  } catch (e) {
    return { status: 0, detail: `réseau/CORS : ${e.message || e}` };
  }
  if (resp.ok) return null;
  let detail = "";
  try { const j = await resp.json(); detail = j?.error?.message || ""; } catch (_) {}
  return { status: resp.status, detail };
}

// Message clair pour un échec de tileset (surtout Google 403).
function formatTilesetError(pf, isGoogle) {
  if (pf.status === 403) {
    return isGoogle
      ? `Google refuse la clé (403). Dans Google Cloud : ① activez « Map Tiles API », ② activez la facturation, ③ autorisez le domaine dans les restrictions de la clé (Référents HTTP : openmapagents.geoafrica.fr/*, et localhost pour le dev).${pf.detail ? ` — Détail : ${pf.detail}` : ""}`
      : `Accès refusé (403) au tileset. Vérifiez la clé/les droits.${pf.detail ? ` — ${pf.detail}` : ""}`;
  }
  if (pf.status === 404) return `Tileset introuvable (404) : vérifiez l'URL du root.json / tileset.json.`;
  if (pf.status === 0) return `Impossible de joindre le tileset (${pf.detail}).`;
  return `Échec du tileset (HTTP ${pf.status})${pf.detail ? ` : ${pf.detail}` : ""}.`;
}

// Formate une erreur de chargement 3D en message clair selon sa nature.
function formatLoadError(err) {
  const m = err?.message || String(err);
  // Limitation loaders.gl : LAS/LAZ version ≤ 1.3 uniquement (votre fichier est 1.4 ?)
  if (/1\.3|1\.4|only version|version file|\blas\b|\blaz\b/i.test(m)) {
    return `Fichier LAS/LAZ non lu (« ${m} »). Seules les versions LAS ≤ 1.3 sont supportées par le lecteur ; convertissez votre fichier en LAS 1.2/1.3 — ex. PDAL : pdal translate in.laz out.laz --writers.las.minor_version=2, ou LAStools « las2las -set_version 1.2 », ou CloudCompare (Save as → LAS 1.2).`;
  }
  // Module 3D absent (npm install manquant)
  if (/dynamically imported module|failed to fetch|cannot find module|mime type|importing a module/i.test(m)) {
    return `Module 3D introuvable (« ${m} »). Lancez « npm install » puis rebuild.`;
  }
  return `Erreur : ${m}`;
}

export default function Scene3DPanel({ mapRef, onAddLayer }) {
  const C = useThemeContext();

  const [globe, setGlobeOn] = useState(false);
  const [kind, setKind] = useState("3dtiles");
  const [url, setUrl] = useState("");
  const [googleKey, setGoogleKey] = useState("");   // clé API Google (3D Tiles photoréalistes)
  const [lon, setLon] = useState("");
  const [lat, setLat] = useState("");
  const [alt, setAlt] = useState("0");
  const [sizeScale, setSizeScale] = useState("1");
  const [pointSize, setPointSize] = useState("2");
  const [opacity, setOpacity] = useState(0.95);
  const [layers, setLayers] = useState([]);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState(null);
  const [splatUrl, setSplatUrl] = useState(null);
  const fileRef = useRef(null);

  // Nuage de points actif (contrôles densité / colorisation)
  const [pc, setPc] = useState(null);            // { id, fileName, full, anchor, version, histogram }
  const [pcDisplay, setPcDisplay] = useState(150000);
  const [pcColorMode, setPcColorMode] = useState("rgb"); // rgb | class | elevation | uniform
  const [classOverrides, setClassOverrides] = useState({}); // { classNum: "#rrggbb" }
  const [classSel, setClassSel] = useState(null);           // Set des classes visibles (null = toutes)

  const kindDef = KINDS.find(k => k.id === kind);

  const getMap = useCallback(() => mapRef.current?.getMap?.() || null, [mapRef]);

  // (Re)construit la couche du nuage actif selon filtre de classes + densité +
  // colorisation. `overrides` = couleurs de classe ; `sel` = Set des classes
  // visibles (défaut : états classOverrides / classSel).
  const applyPC = useCallback((pcObj, display, mode, psize, overrides, sel) => {
    const map = getMap();
    if (!map || !pcObj) return;
    // Filtre par classe (avant sous-échantillonnage → les classes rares restent
    // représentées). Ignoré si toutes les classes sont sélectionnées.
    const selSet = sel !== undefined ? sel : classSel;
    const nClasses = pcObj.histogram ? Object.keys(pcObj.histogram).length : 0;
    let base = pcObj.full;
    if (selSet && base.classification && selSet.size < nClasses)
      base = filterByClass(base, selSet);

    const sub = subsamplePC(base, display);
    let colors = null;
    if (mode === "rgb" && sub.rgb) colors = sub.rgb;
    else if (mode === "class" && sub.classification)
      colors = classColors(sub.classification, buildClassTable(overrides ?? classOverrides));
    else if (mode === "elevation") colors = elevationColors(sub.positions, sub.count);
    add3DPointCloudData(map, {
      id: pcObj.id, name: pcObj.fileName, count: sub.count,
      positions: sub.positions, colors, anchor: pcObj.anchor,
      pointSize: psize, opacity,
    });
    setLayers(list3DLayers());
  }, [getMap, opacity, classOverrides, classSel]);

  // État initial : projection courante + pré-remplir lon/lat au centre carte
  useEffect(() => {
    const map = getMap();
    if (!map) return;
    setGlobeOn(isGlobe(map));
    const c = map.getCenter();
    setLon(c.lng.toFixed(5)); setLat(c.lat.toFixed(5));
    setLayers(list3DLayers());
  }, [getMap]);

  const refresh = () => setLayers(list3DLayers());

  const toggleGlobe = () => {
    const map = getMap();
    if (!map) return;
    const next = !globe;
    const ok = setGlobe(map, next);
    if (ok) {
      setGlobeOn(next);
      if (next) { try { map.easeTo({ pitch: 0, duration: 600 }); } catch (_) {} }
    } else {
      setStatus({ type: "error", msg: "Projection globe non supportée (MapLibre < 5 ?)" });
    }
  };

  const useMapCenter = () => {
    const map = getMap(); if (!map) return;
    const c = map.getCenter();
    setLon(c.lng.toFixed(5)); setLat(c.lat.toFixed(5));
  };

  const handleAdd = async () => {
    const map = getMap();
    if (!map) { setStatus({ type: "error", msg: "Carte non prête" }); return; }

    const gkey = googleKey.trim();
    // 3D Tiles : URL du tileset OU (pour Google) simplement une clé API.
    let turl = url.trim();
    let apiKey = null;
    if (kind === "3dtiles") {
      // Clé collée dans le champ URL par erreur → on la bascule en clé Google.
      if (!gkey && /^AIza[\w-]{10,}$/.test(turl)) { apiKey = turl; turl = ""; }
      else if (gkey) apiKey = gkey;
      if (!turl && apiKey) turl = GOOGLE_3DTILES_URL;   // clé seule → endpoint Google
      if (apiKey) turl = stripKeyParam(turl);           // on authentifie par en-tête
      if (!turl) { setStatus({ type: "error", msg: "Saisissez l'URL d'un tileset.json (ou une clé API Google)." }); return; }
    } else if (!turl) {
      setStatus({ type: "error", msg: "Saisissez une URL" }); return;
    }

    // Gaussian splat → visualiseur autonome
    if (kind === "splat") { setSplatUrl(turl); return; }

    const id = `d3_${kind}_${Date.now()}`;
    const def = { id, kind, url: turl, apiKey, name: turl.split("/").pop() || kind };
    if (kindDef.placed) {
      const plon = parseFloat(lon), plat = parseFloat(lat);
      if (Number.isNaN(plon) || Number.isNaN(plat)) {
        setStatus({ type: "error", msg: "Longitude / latitude invalides" }); return;
      }
      def.position = [plon, plat, parseFloat(alt) || 0];
      def.pointSize = parseFloat(pointSize) || 2;
      if (kind === "model") def.sizeScale = parseFloat(sizeScale) || 1;
    }

    try {
      setBusy(true);
      // deck.gl (MapboxOverlay) rend en mercator ; sous le globe MapLibre son
      // overlay ne s'affiche pas → on repasse en mercator pour voir la 3D.
      if (isGlobe(map)) { setGlobe(map, false); setGlobeOn(false); }
      // Pré-vol : on valide le tileset (root.json) pour capter le 403 Google
      // au lieu de le laisser filer en rejet promise dans la console.
      if (kind === "3dtiles") {
        const isGoogle = /tile\.googleapis\.com/.test(turl);
        setStatus({ type: "info", msg: "Vérification du tileset…" });
        const pf = await preflight3DTiles(turl, apiKey);
        // On bloque sur un refus net (403/404) ou tout échec Google ; un aléa
        // réseau/CORS sur un service libre → on tente quand même via deck.gl.
        if (pf && (isGoogle || pf.status === 403 || pf.status === 404)) {
          setStatus({ type: "error", msg: formatTilesetError(pf, isGoogle) }); setBusy(false); return;
        }
        if (pf) console.warn("Pré-vol 3D Tiles non concluant, tentative directe :", pf);
      }
      setStatus({ type: "info", msg: "Chargement de la couche 3D…" });
      await add3DLayer(map, def);
      refresh();
      setStatus({
        type: "ok",
        msg: kind === "3dtiles"
          ? "Tileset ajouté — zoomez/inclinez sur une ville : les bâtiments se chargent à l'échelle rue."
          : "Couche 3D ajoutée",
      });
      setUrl("");
    } catch (e) {
      setStatus({ type: "error", msg: formatLoadError(e) });
    }
    setBusy(false);
  };

  // Import d'un fichier 3D local (.las/.laz, .glb/.gltf, .ply/.splat/.ksplat)
  const onPickFile = async (e) => {
    const file = e.target.files?.[0];
    e.target.value = "";                       // permet de re-choisir le même fichier
    if (!file) return;
    const map = getMap();
    if (!map) { setStatus({ type: "error", msg: "Carte non prête" }); return; }

    const ext = file.name.toLowerCase().split(".").pop();

    // Gaussian splat → visualiseur plein écran
    if (["ply", "splat", "ksplat"].includes(ext)) { setSplatUrl(URL.createObjectURL(file)); return; }

    const c = map.getCenter();
    const plon = parseFloat(lon), plat = parseFloat(lat);
    const anchor = [Number.isNaN(plon) ? c.lng : plon, Number.isNaN(plat) ? c.lat : plat, parseFloat(alt) || 0];

    // ── LiDAR LAS/LAZ → conversion serveur (laspy : toutes versions 1.0–1.4) ──
    if (["las", "laz"].includes(ext)) {
      setKind("pointcloud");
      try {
        setBusy(true);

        // ── .las non compressé volumineux : réduction LOCALE avant upload ──
        // (évite d'envoyer 177 Mo ; on n'upload qu'une fraction des points)
        let upFile = file;
        if (ext === "las" && file.size > 8 * 1048576) {
          setStatus({ type: "info", msg: `Préparation locale de ${file.name}…` });
          try {
            const small = await buildSmallLas(file, 250000);
            if (small) upFile = small;
          } catch (_) { /* repli : upload du fichier complet */ }
        }

        const mb = (upFile.size / 1048576).toFixed(1);
        const reduced = upFile !== file ? ` (réduit de ${(file.size / 1048576).toFixed(0)} Mo)` : "";
        setStatus({ type: "info", msg: `Envoi de ${file.name} (${mb} Mo${reduced})…` });
        // Upload avec progression : on voit l'envoi puis la conversion serveur
        const d = await uploadLidar(upFile, (p) => {
          setStatus({
            type: "info",
            msg: p < 1 ? `Envoi… ${Math.round(p * 100)}% (${mb} Mo${reduced})`
                       : "Conversion sur le serveur (échantillonnage)…",
          });
        });
        const positions = new Float32Array(_b64ToBuf(d.positions_b64));
        const colors = d.colors_b64 ? new Uint8Array(_b64ToBuf(d.colors_b64)) : null;
        const classification = d.classification_b64 ? new Uint8Array(_b64ToBuf(d.classification_b64)) : null;

        // Ancrage : centroïde reprojeté en lon/lat par le serveur (placement correct)
        const pcAnchor = d.center_lonlat
          ? [d.center_lonlat[0], d.center_lonlat[1], 0]
          : anchor;

        const pcObj = {
          id: `d3_pc_${Date.now()}`, fileName: file.name, version: d.version,
          crs: d.crs || null, histogram: d.class_histogram || {},
          anchor: pcAnchor,
          full: { positions, rgb: colors, classification, count: d.count },
        };
        // Colorise par défaut sur la classification si présente (plus utile
        // qu'un RGB souvent absent), sinon RGB, sinon élévation.
        const mode = classification ? "class" : (colors ? "rgb" : "elevation");
        const disp = Math.min(d.count, 150000);
        // Toutes les classes visibles au départ
        const sel0 = new Set(Object.keys(pcObj.histogram).map(Number));
        setClassOverrides({});
        setClassSel(sel0);
        setPc(pcObj); setPcColorMode(mode); setPcDisplay(disp);
        applyPC(pcObj, disp, mode, parseFloat(pointSize) || 2, {}, sel0);

        const tot = d.total > d.count ? ` (sur ${d.total.toLocaleString()})` : "";
        if (d.center_lonlat) {
          setStatus({ type: "ok", msg: `${file.name} : ${d.count.toLocaleString()} pts${tot} · LAS ${d.version} · ${d.crs || "géoréférencé"}` });
        } else {
          // Placement non géoréférencé → on prévient clairement (cause = CRS
          // illisible ou pyproj absent côté serveur) au lieu d'atterrir « dans l'océan ».
          setStatus({ type: "error", msg: `${file.name} chargé mais NON géoréférencé (placé au centre de la carte). Raison : ${d.georef_error || "CRS absent du fichier"}. ${d.crs ? "" : "Vérifiez que le .las/.laz contient sa projection."}` });
        }
      } catch (err) {
        // Le backend renvoie déjà des messages clairs (well-log, LAZ, vide…)
        setStatus({ type: "error", msg: err?.message || String(err) });
      }
      setBusy(false);
      return;
    }

    // ── glTF / GLB → modèle (deck.gl ScenegraphLayer, côté client) ──
    if (["glb", "gltf"].includes(ext)) {
      setKind("model");
      const objUrl = URL.createObjectURL(file);
      const def = {
        id: `d3_model_${Date.now()}`, kind: "model", url: objUrl, name: file.name,
        position: anchor, sizeScale: parseFloat(sizeScale) || 1,
      };
      try {
        setBusy(true); setStatus({ type: "info", msg: `Chargement de ${file.name}…` });
        await add3DLayer(map, def);
        refresh();
        setStatus({ type: "ok", msg: `${file.name} ajouté` });
      } catch (err) {
        setStatus({ type: "error", msg: formatLoadError(err) });
        URL.revokeObjectURL(objUrl);
      }
      setBusy(false);
      return;
    }

    setStatus({ type: "error", msg: `Extension « .${ext} » non gérée (attendu : las, laz, glb, gltf, ply, splat).` });
  };

  const handleRemove = async (id) => {
    const map = getMap(); if (!map) return;
    await remove3DLayer(map, id); refresh();
    if (pc && pc.id === id) setPc(null);   // ferme les contrôles du nuage actif
  };
  const handleToggle = async (id, vis) => {
    const map = getMap(); if (!map) return;
    await set3DVisible(map, id, vis); refresh();
  };
  const handleOpacity = async (id, op) => {
    const map = getMap(); if (!map) return;
    await set3DOpacity(map, id, op); refresh();
  };

  const inp = {
    fontFamily: M, fontSize: 10, padding: "5px 7px", borderRadius: 5,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };
  const statColor = { ok: C.acc, error: C.red, info: C.amb };

  return (
    <>
      <div style={{ flex: 1, overflowY: "auto", padding: 10, display: "flex", flexDirection: "column", gap: 12 }}>

        {/* ── Vue Globe ───────────────────────────────────────── */}
        <div style={{ background: C.hover, borderRadius: 8, padding: "10px 12px", border: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", gap: 10 }}>
          <IcGlobe size={20} color={C.acc}/>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 12, fontWeight: 600, color: C.txt }}>Vue Globe 3D</div>
            <div style={{ fontSize: 9, color: C.dim }}>Projection sphérique du monde</div>
          </div>
          <button onClick={toggleGlobe} style={{
            width: 42, height: 22, borderRadius: 11, border: "none", cursor: "pointer", position: "relative",
            background: globe ? C.acc : C.bdr, transition: "background .2s",
          }}>
            <span style={{
              position: "absolute", top: 2, left: globe ? 22 : 2, width: 18, height: 18, borderRadius: "50%",
              background: "#fff", transition: "left .2s",
            }} />
          </button>
        </div>

        {/* ── Ajout de couche 3D ─────────────────────────────── */}
        <div>
          <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 5 }}>Ajouter des données 3D</div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 4, marginBottom: 7 }}>
            {KINDS.map(k => (
              <button key={k.id} onClick={() => setKind(k.id)} style={{
                fontFamily: F, fontSize: 10, padding: "6px 6px", borderRadius: 6, cursor: "pointer",
                display: "flex", alignItems: "center", gap: 6, textAlign: "left",
                background: kind === k.id ? C.acc + "18" : "transparent",
                border: `0.5px solid ${kind === k.id ? C.acc + "66" : C.bdr}`,
                color: kind === k.id ? C.acc : C.txt,
              }}>
                {k.icon && <k.icon size={14}/>}{k.label}
              </button>
            ))}
          </div>

          <div style={{ fontSize: 9, color: C.dim, marginBottom: 4 }}>{kindDef?.hint}</div>
          <input value={url} onChange={e => setUrl(e.target.value)} placeholder="https://… (URL de la ressource)" style={inp} />

          {/* Import de fichier local (sauf 3D Tiles qui est multi-fichiers) */}
          {kind !== "3dtiles" && (
            <>
              <div style={{ display: "flex", alignItems: "center", gap: 8, margin: "7px 0 0" }}>
                <div style={{ flex: 1, height: 1, background: C.bdr }} />
                <span style={{ fontSize: 9, color: C.dim }}>ou fichier local</span>
                <div style={{ flex: 1, height: 1, background: C.bdr }} />
              </div>
              <button onClick={() => fileRef.current?.click()} disabled={busy} style={{
                marginTop: 6, fontFamily: F, fontSize: 11, padding: "8px 0", borderRadius: 6, width: "100%",
                background: "transparent", border: `1px dashed ${C.acc}77`, color: C.acc, cursor: busy ? "default" : "pointer",
                display: "flex", alignItems: "center", justifyContent: "center", gap: 7,
              }}><IcUpload size={13}/> Choisir un fichier (.las .laz .glb .gltf .ply .splat)</button>
            </>
          )}
          {kind === "3dtiles" && (
            <div style={{ marginTop: 6, display: "flex", flexDirection: "column", gap: 5 }}>
              <div style={{ fontSize: 9, color: C.dim, fontStyle: "italic" }}>
                URL d'un <b>tileset.json</b>, ou un service prêt à l'emploi :
              </div>
              {/* Services gratuits sans clé */}
              {TILES3D_PRESETS.map(p => (
                <button key={p.url} onClick={() => { setUrl(p.url); setGoogleKey(""); }} style={{
                  fontFamily: F, fontSize: 10, padding: "6px 8px", borderRadius: 6, width: "100%", textAlign: "left",
                  background: url === p.url ? C.acc + "18" : "transparent",
                  border: `1px dashed ${url === p.url ? C.acc : C.acc + "77"}`, color: C.acc, cursor: "pointer",
                }}>{p.label}</button>
              ))}
              {/* Google (clé requise) */}
              <button onClick={() => { setUrl(GOOGLE_3DTILES_URL); }} style={{
                fontFamily: F, fontSize: 10, padding: "6px 8px", borderRadius: 6, width: "100%", textAlign: "left",
                background: url === GOOGLE_3DTILES_URL ? C.acc + "18" : "transparent",
                border: `1px dashed ${url === GOOGLE_3DTILES_URL ? C.acc : C.acc + "77"}`, color: C.acc, cursor: "pointer",
                display: "flex", alignItems: "center", gap: 6,
              }}><IcGlobe size={13}/> Google 3D Tiles photoréalistes (clé requise)</button>
              {/* Champ clé : seulement utile pour Google */}
              {url === GOOGLE_3DTILES_URL && (
                <div>
                  <div style={{ fontSize: 8, color: C.dim, marginBottom: 2 }}>Clé API Google (Map Tiles API)</div>
                  <input value={googleKey} onChange={e => setGoogleKey(e.target.value)} placeholder="AIza… (clé Map Tiles API)"
                    autoComplete="off" spellCheck={false} style={inp} />
                  <div style={{ fontSize: 8, color: C.dim, marginTop: 3, lineHeight: 1.5 }}>
                    Nécessite : <b>Map Tiles API</b> activée + <b>facturation</b> active, et le domaine
                    autorisé dans les restrictions de la clé. La clé part en en-tête (jamais dans l'URL des tuiles).
                  </div>
                </div>
              )}
            </div>
          )}
          <input ref={fileRef} type="file" accept=".las,.laz,.glb,.gltf,.ply,.splat,.ksplat" onChange={onPickFile} style={{ display: "none" }} />

          {/* Position (modèles / nuages) */}
          {kindDef?.placed && (
            <div style={{ marginTop: 7, background: C.hover, borderRadius: 6, padding: 8, border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 6 }}>
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <span style={{ fontSize: 9, color: C.dim, flex: 1 }}>Position</span>
                <button onClick={useMapCenter} style={{ fontFamily: F, fontSize: 9, padding: "2px 7px", borderRadius: 4, background: "transparent", border: `0.5px solid ${C.acc}55`, color: C.acc, cursor: "pointer", display: "inline-flex", alignItems: "center", gap: 4 }}><IcMapPin size={11}/> Centre carte</button>
              </div>
              <div style={{ display: "flex", gap: 5 }}>
                <div style={{ flex: 1 }}><div style={{ fontSize: 8, color: C.dim }}>Lon</div><input value={lon} onChange={e => setLon(e.target.value)} style={inp} /></div>
                <div style={{ flex: 1 }}><div style={{ fontSize: 8, color: C.dim }}>Lat</div><input value={lat} onChange={e => setLat(e.target.value)} style={inp} /></div>
                <div style={{ width: 56 }}><div style={{ fontSize: 8, color: C.dim }}>Alt(m)</div><input value={alt} onChange={e => setAlt(e.target.value)} style={inp} /></div>
              </div>
              <div style={{ display: "flex", gap: 5 }}>
                {kind === "model" && (
                  <div style={{ flex: 1 }}><div style={{ fontSize: 8, color: C.dim }}>Échelle</div><input value={sizeScale} onChange={e => setSizeScale(e.target.value)} style={inp} /></div>
                )}
                <div style={{ flex: 1 }}><div style={{ fontSize: 8, color: C.dim }}>Taille point</div><input value={pointSize} onChange={e => setPointSize(e.target.value)} style={inp} /></div>
              </div>
            </div>
          )}

          <button onClick={handleAdd} disabled={busy} style={{
            marginTop: 8, fontFamily: F, fontSize: 12, fontWeight: 600, padding: "8px 0", borderRadius: 6, width: "100%",
            background: busy ? C.hover : C.acc, color: busy ? C.dim : "#fff", border: "none",
            cursor: busy ? "default" : "pointer", opacity: busy ? 0.7 : 1,
            display: "flex", alignItems: "center", justifyContent: "center", gap: 7,
          }}>
            {kind === "splat" ? <IcSparkles size={14}/> : <IcPlus size={14}/>}
            {busy ? "Chargement…" : kind === "splat" ? "Ouvrir le visualiseur" : "Ajouter à la carte"}
          </button>
        </div>

        {/* ── Statut ──────────────────────────────────────────── */}
        {status && (
          <div style={{
            fontSize: 10, padding: "5px 8px", borderRadius: 5, lineHeight: 1.5,
            background: statColor[status.type] + "15", border: `0.5px solid ${statColor[status.type]}44`, color: statColor[status.type],
          }}>{status.msg}</div>
        )}

        {/* ── Nuage de points : densité + colorisation ────────── */}
        {pc && (
          <div style={{ background: C.hover, borderRadius: 8, padding: "9px 10px", border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 8 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <IcCircleDot size={13} color={C.acc}/>
              <span style={{ fontSize: 11, fontWeight: 600, color: C.txt, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{pc.fileName}</span>
              <span style={{ fontSize: 8, color: C.dim, fontFamily: M }}>LAS {pc.version}</span>
            </div>

            {/* Variable de colorisation (liste déroulante) */}
            <div>
              <div style={{ fontSize: 9, color: C.dim, marginBottom: 3 }}>Variable de couleur</div>
              <select
                value={pcColorMode}
                onChange={e => { const m = e.target.value; setPcColorMode(m); applyPC(pc, pcDisplay, m, parseFloat(pointSize) || 2); }}
                style={{ ...inp, cursor: "pointer" }}
              >
                {pc.full.classification && <option value="class">Classification</option>}
                {pc.full.rgb && <option value="rgb">Couleur RGB (photo)</option>}
                <option value="elevation">Élévation (altitude Z)</option>
                <option value="uniform">Uniforme (bleu)</option>
              </select>
            </div>

            {/* Classes : filtre d'affichage (cases) + recoloration (pastilles) */}
            {pc.full.classification && Object.keys(pc.histogram).length > 0 && (() => {
              const total = Object.values(pc.histogram).reduce((a, b) => a + b, 0) || 1;
              const entries = Object.entries(pc.histogram).sort((a, b) => b[1] - a[1]);
              const allNums = entries.map(([c]) => Number(c));
              const sel = classSel || new Set(allNums);
              const hasOv = Object.keys(classOverrides).length > 0;
              const psize = parseFloat(pointSize) || 2;
              const setSel = (next) => { setClassSel(next); applyPC(pc, pcDisplay, pcColorMode, psize, undefined, next); };
              const toggle = (n) => { const nx = new Set(sel); if (nx.has(n)) nx.delete(n); else nx.add(n); setSel(nx); };
              return (
                <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    <span style={{ fontSize: 9, color: C.dim }}>Classes — cochez pour afficher</span>
                    <div style={{ display: "flex", gap: 4 }}>
                      <button onClick={() => setSel(new Set(allNums))} style={{ fontFamily: F, fontSize: 8, padding: "1px 6px", borderRadius: 4, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim, cursor: "pointer" }}>Tout</button>
                      <button onClick={() => setSel(new Set())} style={{ fontFamily: F, fontSize: 8, padding: "1px 6px", borderRadius: 4, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim, cursor: "pointer" }}>Aucun</button>
                      {hasOv && (
                        <button title="Réinitialiser les couleurs" onClick={() => { setClassOverrides({}); applyPC(pc, pcDisplay, pcColorMode, psize, {}); }}
                          style={{ fontFamily: F, fontSize: 8, padding: "1px 6px", borderRadius: 4, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim, cursor: "pointer" }}>↺ Couleurs</button>
                      )}
                    </div>
                  </div>
                  <div style={{ display: "flex", flexDirection: "column", gap: 2, maxHeight: 160, overflowY: "auto" }}>
                    {entries.map(([cls, cnt]) => {
                      const n = Number(cls);
                      const info = ASPRS_CLASSES[cls] || [`Classe ${cls}`, CLASS_FALLBACK];
                      const hex = classOverrides[cls] || info[1];
                      const on = sel.has(n);
                      return (
                        <div key={cls} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 9, opacity: on ? 1 : 0.4 }}>
                          <input type="checkbox" checked={on} onChange={() => toggle(n)}
                            title={on ? "Masquer cette classe" : "Afficher cette classe"}
                            style={{ width: 12, height: 12, flexShrink: 0, cursor: "pointer", accentColor: C.acc }} />
                          <label title="Changer la couleur" style={{ width: 14, height: 14, borderRadius: 3, background: hex, flexShrink: 0, border: "0.5px solid rgba(0,0,0,.2)", cursor: "pointer", position: "relative", overflow: "hidden" }}>
                            <input type="color" value={hex}
                              onChange={e => {
                                const next = { ...classOverrides, [cls]: e.target.value };
                                setClassOverrides(next);
                                applyPC(pc, pcDisplay, pcColorMode, psize, next);
                              }}
                              style={{ position: "absolute", inset: -4, opacity: 0, cursor: "pointer", border: "none", padding: 0 }} />
                          </label>
                          <span style={{ color: C.mut, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                            {info[0]} <span style={{ color: C.dim, fontFamily: M }}>· cl.{cls}</span>
                          </span>
                          <span style={{ color: C.dim, fontFamily: M }}>{(cnt / total * 100).toFixed(0)}%</span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })()}

            {/* Densité d'affichage */}
            <div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.dim, marginBottom: 2 }}>
                <span>Points affichés</span>
                <span style={{ fontFamily: M, color: C.txt }}>{pcDisplay.toLocaleString()} / {pc.full.count.toLocaleString()}</span>
              </div>
              <input type="range" min={Math.min(2000, pc.full.count)} max={pc.full.count}
                step={Math.max(500, Math.round(pc.full.count / 50))}
                value={Math.min(pcDisplay, pc.full.count)}
                onChange={e => setPcDisplay(parseInt(e.target.value))}
                onMouseUp={e => applyPC(pc, parseInt(e.target.value), pcColorMode, parseFloat(pointSize) || 2)}
                onTouchEnd={e => applyPC(pc, parseInt(e.target.value), pcColorMode, parseFloat(pointSize) || 2)}
                style={{ width: "100%", height: 3 }} />
              <div style={{ fontSize: 8, color: C.dim, marginTop: 1 }}>Réduisez pour fluidifier l'affichage.</div>
            </div>

            {/* Taille des points */}
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ fontSize: 9, color: C.dim, flexShrink: 0 }}>Taille point</span>
              <input type="range" min="1" max="8" step="0.5" value={pointSize}
                onChange={e => { setPointSize(e.target.value); applyPC(pc, pcDisplay, pcColorMode, parseFloat(e.target.value) || 2); }}
                style={{ flex: 1, height: 3 }} />
              <span style={{ fontFamily: M, fontSize: 9, color: C.txt }}>{pointSize}</span>
            </div>

            {/* Opacité */}
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ fontSize: 9, color: C.dim, flexShrink: 0 }}>Opacité</span>
              <input type="range" min="0.1" max="1" step="0.05" value={opacity}
                onChange={e => setOpacity(parseFloat(e.target.value))}
                onMouseUp={() => applyPC(pc, pcDisplay, pcColorMode, parseFloat(pointSize) || 2)}
                onTouchEnd={() => applyPC(pc, pcDisplay, pcColorMode, parseFloat(pointSize) || 2)}
                style={{ flex: 1, height: 3 }} />
              <span style={{ fontFamily: M, fontSize: 9, color: C.txt }}>{Math.round(opacity * 100)}%</span>
            </div>
          </div>
        )}

        {/* La foresterie (MNT/MNS/MNH, arbres) est dans le menu LiDAR dédié. */}

        {/* ── Couches 3D actives ──────────────────────────────── */}
        {layers.length > 0 && (
          <div>
            <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 5 }}>Couches 3D ({layers.length})</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
              {layers.map(l => (
                <div key={l.id} style={{ background: C.hover, borderRadius: 6, padding: "6px 8px", border: `0.5px solid ${C.bdr}` }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                    {(() => { const KI = KIND_ICON[l.kind] || IcCube; return <KI size={13} color={C.mut}/>; })()}
                    <span style={{ fontSize: 10, color: C.txt, flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{l.name}</span>
                    <button onClick={() => handleToggle(l.id, !l.visible)} title="Visibilité" style={{ background: "transparent", border: "none", cursor: "pointer", color: l.visible ? C.acc : C.dim, display: "flex", padding: 1 }}>{l.visible ? <IcEye size={13}/> : <IcEyeOff size={13}/>}</button>
                    <button onClick={() => handleRemove(l.id)} title="Supprimer" style={{ background: "transparent", border: "none", cursor: "pointer", color: C.red, display: "flex", padding: 1 }}><IcX size={13}/></button>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 6, marginTop: 4 }}>
                    <span style={{ fontSize: 8, color: C.dim }}>Opacité</span>
                    <input type="range" min="0.1" max="1" step="0.05" value={l.opacity}
                      onChange={e => handleOpacity(l.id, parseFloat(e.target.value))} style={{ flex: 1, height: 3 }} />
                    <span style={{ fontFamily: M, fontSize: 9, color: C.txt }}>{Math.round(l.opacity * 100)}%</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        <div style={{ fontSize: 9, color: C.dim, lineHeight: 1.5, marginTop: "auto", paddingTop: 8, borderTop: `0.5px solid ${C.bdr}`, display: "flex", gap: 6 }}>
          <IcInfo size={12} style={{ flexShrink: 0, marginTop: 1 }}/>
          <span>Astuce : inclinez la carte (clic droit + glisser, ou Ctrl+glisser) pour mieux voir le relief 3D.
          Les 3D Tiles se géoréférencent seules ; les modèles et nuages se placent sur le point indiqué.</span>
        </div>
      </div>

      {splatUrl && <SplatViewerModal url={splatUrl} onClose={() => setSplatUrl(null)} />}
    </>
  );
}
