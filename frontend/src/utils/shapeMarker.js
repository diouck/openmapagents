/**
 * shapeMarker.js — marqueurs ponctuels NON ronds (carré, triangle, losange) pour MapLibre.
 * Comme makiLoader : canvas 2D → Uint8Array → map.addImage({ width, height, data }, { pixelRatio }).
 * Chaque marqueur porte un aplat (fill) + un contour (outline) d'épaisseur variable — façon QGIS.
 * L'id d'image encode tous les paramètres → régénéré seulement quand un paramètre change.
 */

const safe = (c) => String(c || "").replace("#", "").toLowerCase();

export function shapeMarkerId(shape, fill, outline, strokeWidth, size) {
  return `mk_${shape}_${safe(fill)}_${safe(outline)}_${Math.round((strokeWidth || 0) * 10)}_${Math.round(size)}`;
}

/**
 * Charge (si absent) un marqueur de forme dans la map — SYNCHRONE.
 * @returns imageId ou null si forme ronde / erreur.
 */
export function loadShapeMarker(map, shape, fill = "#3388ff", outline = "#000000", strokeWidth = 1.5, size = 12) {
  if (!map || !shape || shape === "circle") return null;
  const id = shapeMarkerId(shape, fill, outline, strokeWidth, size);
  if (map.hasImage(id)) return id;

  const pr = 2;                                   // netteté (device pixels)
  const sw = Math.max(0, strokeWidth);
  const pad = Math.ceil(sw) + 2;                  // marge pour le contour + anti-crénelage
  const S = Math.max(4, Math.round(size)) + pad * 2;
  const canvas = document.createElement("canvas");
  canvas.width = S * pr; canvas.height = S * pr;
  const ctx = canvas.getContext("2d");
  ctx.scale(pr, pr);
  ctx.clearRect(0, 0, S, S);
  ctx.lineJoin = "miter"; ctx.miterLimit = 10; ctx.lineCap = "butt";

  const cx = S / 2, cy = S / 2, r = size / 2;
  ctx.beginPath();
  if (shape === "square") {
    ctx.rect(cx - r, cy - r, r * 2, r * 2);
  } else if (shape === "triangle") {
    ctx.moveTo(cx, cy - r);
    ctx.lineTo(cx + r * 0.92, cy + r * 0.8);
    ctx.lineTo(cx - r * 0.92, cy + r * 0.8);
    ctx.closePath();
  } else if (shape === "diamond") {
    ctx.moveTo(cx, cy - r); ctx.lineTo(cx + r, cy); ctx.lineTo(cx, cy + r); ctx.lineTo(cx - r, cy); ctx.closePath();
  } else {
    ctx.rect(cx - r, cy - r, r * 2, r * 2);       // repli = carré
  }
  ctx.fillStyle = fill; ctx.fill();
  if (sw > 0) { ctx.lineWidth = sw; ctx.strokeStyle = outline; ctx.stroke(); }

  const imageData = ctx.getImageData(0, 0, S * pr, S * pr);
  try {
    map.addImage(id, { width: S * pr, height: S * pr, data: new Uint8Array(imageData.data.buffer) }, { pixelRatio: pr });
    return id;
  } catch (e) {
    return null;
  }
}
