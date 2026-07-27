/**
 * lidarStyle.js — Coloration, filtrage par classe et restyle des nuages de
 * points (deck.gl), pilotables depuis le menu Couches.
 *
 * Un magasin module garde, par id de couche, les données complètes du nuage
 * (positions / rgb / classification) + le style courant. `applyPCStyle` recolore
 * (avec filtre) et ré-alimente le PointCloudLayer via deck3d.
 */
import { add3DPointCloudData, set3DOpacity } from "./deck3d";

// Classes ASPRS (classification LiDAR standard) → [libellé, couleur]
export const ASPRS_CLASSES = {
  0: ["Jamais classé", "#bdbdbd"], 1: ["Non classé", "#9e9e9e"], 2: ["Sol", "#8c6d31"],
  3: ["Végétation basse", "#c2e699"], 4: ["Végétation moy.", "#78c679"], 5: ["Végétation haute", "#238443"],
  6: ["Bâtiment", "#e31a1c"], 7: ["Bruit bas", "#fb9a99"], 8: ["Clé modèle", "#dd3497"], 9: ["Eau", "#1f78b4"],
  10: ["Voie ferrée", "#6a3d9a"], 11: ["Route", "#525252"], 12: ["Couvert", "#969696"],
  13: ["Câble (garde)", "#ff7f00"], 14: ["Câble (cond.)", "#ffbf00"], 15: ["Pylône", "#b15928"],
  16: ["Connecteur", "#fdbf6f"], 17: ["Pont", "#cab2d6"], 18: ["Bruit haut", "#fb9a99"],
};
const FALLBACK = "#bdbdbd";

export function hexToRgb(hex) {
  const h = (hex || FALLBACK).replace("#", "");
  return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)];
}
const CLASS_RGB = Array.from({ length: 256 }, (_, i) => hexToRgb(ASPRS_CLASSES[i]?.[1] || FALLBACK));

export function buildClassTable(overrides) {
  if (!overrides || !Object.keys(overrides).length) return CLASS_RGB;
  const t = CLASS_RGB.map(c => c.slice());
  for (const [k, hex] of Object.entries(overrides)) {
    const i = Number(k);
    if (i >= 0 && i < 256 && hex) t[i] = hexToRgb(hex);
  }
  return t;
}
function classColors(cls, table) {
  const t = table || CLASS_RGB;
  const n = cls.length; const out = new Uint8Array(n * 3);
  for (let i = 0; i < n; i++) { const c = t[cls[i]] || t[0] || CLASS_RGB[0]; out[i * 3] = c[0]; out[i * 3 + 1] = c[1]; out[i * 3 + 2] = c[2]; }
  return out;
}
const RAMP = [[68, 1, 84], [59, 82, 139], [33, 145, 140], [94, 201, 98], [253, 231, 37]];
function rampColor(x) {
  x = x < 0 ? 0 : x > 1 ? 1 : x;
  const s = x * (RAMP.length - 1); const i = Math.floor(s), f = s - i;
  const a = RAMP[i], b = RAMP[Math.min(i + 1, RAMP.length - 1)];
  return [Math.round(a[0] + (b[0] - a[0]) * f), Math.round(a[1] + (b[1] - a[1]) * f), Math.round(a[2] + (b[2] - a[2]) * f)];
}
function elevationColors(pos, count) {
  let zmin = Infinity, zmax = -Infinity;
  for (let i = 0; i < count; i++) { const z = pos[i * 3 + 2]; if (z < zmin) zmin = z; if (z > zmax) zmax = z; }
  const span = (zmax - zmin) || 1; const out = new Uint8Array(count * 3);
  for (let i = 0; i < count; i++) { const [r, g, b] = rampColor((pos[i * 3 + 2] - zmin) / span); out[i * 3] = r; out[i * 3 + 1] = g; out[i * 3 + 2] = b; }
  return out;
}
function filterByClass(full, selSet) {
  const { positions, rgb, classification, count } = full;
  const idx = new Int32Array(count); let k = 0;
  for (let i = 0; i < count; i++) if (selSet.has(classification[i])) idx[k++] = i;
  const pos = new Float32Array(k * 3); const col = rgb ? new Uint8Array(k * 3) : null; const cls = new Uint8Array(k);
  for (let j = 0; j < k; j++) {
    const i = idx[j];
    pos[j * 3] = positions[i * 3]; pos[j * 3 + 1] = positions[i * 3 + 1]; pos[j * 3 + 2] = positions[i * 3 + 2];
    if (col) { col[j * 3] = rgb[i * 3]; col[j * 3 + 1] = rgb[i * 3 + 1]; col[j * 3 + 2] = rgb[i * 3 + 2]; }
    cls[j] = classification[i];
  }
  return { positions: pos, rgb: col, classification: cls, count: k };
}

// ── Magasin par couche ─────────────────────────────────────────────────────
const _store = new Map();   // id -> { full, histogram, anchor, name, style }

export function registerPC(id, data) {
  _store.set(id, {
    full: data.full, histogram: data.histogram || {}, anchor: data.anchor, name: data.name,
    style: {
      colorMode: data.full.classification ? "class" : (data.full.rgb ? "rgb" : "elevation"),
      classOverrides: {}, classSel: null, pointSize: 2, opacity: 1,
    },
  });
}
export function getPC(id) { return _store.get(id) || null; }
export function removePC(id) { _store.delete(id); }

export async function applyPCStyle(map, id, patch) {
  const e = _store.get(id);
  if (!e || !map) return;
  Object.assign(e.style, patch || {});
  const st = e.style;
  let base = e.full;
  const nClasses = Object.keys(e.histogram).length;
  if (st.classSel && base.classification && st.classSel.size < nClasses) base = filterByClass(base, st.classSel);
  let colors = null;
  if (st.colorMode === "rgb" && base.rgb) colors = base.rgb;
  else if (st.colorMode === "class" && base.classification) colors = classColors(base.classification, buildClassTable(st.classOverrides));
  else if (st.colorMode === "elevation") colors = elevationColors(base.positions, base.count);
  await add3DPointCloudData(map, {
    id, name: e.name, count: base.count, positions: base.positions, colors,
    anchor: e.anchor, pointSize: st.pointSize, opacity: st.opacity,
    fly: false,   // restyle : ne pas re-centrer la caméra
  });
}

// Opacité seule (rapide : pas de recalcul des couleurs)
export function setPCOpacity(map, id, op) {
  const e = _store.get(id); if (e) e.style.opacity = op;
  try { set3DOpacity(map, id, op); } catch (_) {}
}
