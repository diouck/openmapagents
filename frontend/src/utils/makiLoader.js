/**
 * makiLoader.js — Charge les icônes Maki dans MapLibre
 * Méthode : canvas 2D → Uint8Array → map.addImage({ width, height, data })
 * C'est le format que MapLibre accepte de façon synchrone et fiable.
 */
import { MAKI_PATHS } from "./makiIcons";

/**
 * Rend un chemin SVG Maki sur un canvas et retourne { width, height, data }
 * format accepté par map.addImage() de MapLibre GL JS
 */
function renderToImageData(name, color, size) {
  const paths = MAKI_PATHS[name];
  if (!paths || !paths.length) return null;

  const canvas = document.createElement("canvas");
  canvas.width  = size;
  canvas.height = size;
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, size, size);

  // Scale 15→size
  ctx.save();
  ctx.scale(size / 15, size / 15);
  ctx.fillStyle = color;
  for (const d of paths) {
    try { ctx.fill(new Path2D(d)); } catch(e) { console.warn("Path2D error:", name, e.message); }
  }
  ctx.restore();

  // MapLibre veut Uint8Array (pas Uint8ClampedArray)
  const imageData = ctx.getImageData(0, 0, size, size);
  return {
    width:  size,
    height: size,
    data:   new Uint8Array(imageData.data.buffer),
  };
}

/**
 * Charge une icône Maki dans la map MapLibre — SYNCHRONE
 * Retourne l'imageId si succès, null si erreur
 */
export function loadMakiIcon(map, name, color = "#ffffff", size = 30) {
  if (!map || !name) return null;
  if (!MAKI_PATHS[name]) { console.warn(`Maki: icône "${name}" inconnue`); return null; }

  // ID stable et valide (pas d'# ou caractères spéciaux)
  const safeColor = color.replace("#", "").toLowerCase();
  const imageId   = `maki_${name}_${safeColor}_${size}`;

  // Déjà chargée
  if (map.hasImage(imageId)) return imageId;

  const imgData = renderToImageData(name, color, size);
  if (!imgData) return null;

  try {
    map.addImage(imageId, imgData);
    return imageId;
  } catch (e) {
    console.error("makiLoader map.addImage error:", imageId, e.message);
    return null;
  }
}

/**
 * Preview SVG data URL pour le DOM (ClassPanel, Legend) sans map
 */
export function makiToDataUrl(name, color = "#1D9E75") {
  const paths = MAKI_PATHS[name];
  if (!paths) return null;
  const pathsStr = paths.map(d => `<path d="${d}" fill="${color}"/>`).join("");
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 15 15">${pathsStr}</svg>`;
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
}

// Compat — version async qui wrape le sync
export function loadMakiIconAsync(map, name, color = "#ffffff", size = 30) {
  return Promise.resolve(loadMakiIcon(map, name, color, size));
}
