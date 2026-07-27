/**
 * chartSprites.js — Graphiques par entité, rasterisés en images MapLibre.
 *
 * Pourquoi des images et non des marqueurs DOM : un marqueur par entité
 * s'effondre au-delà de quelques centaines d'objets, et il faudrait réécrire la
 * détection de chevauchement. Une couche `symbol` alimentée par des images
 * enregistrées via map.addImage() est rendue sur le GPU, et MapLibre gère
 * lui-même la collision (icon-allow-overlap) et le tri de priorité.
 *
 * Aucune bibliothèque de graphiques : un camembert de 64 px est une suite
 * d'`arc()`, une barre un `rect()`. Seul le pavage du treemap justifie une
 * dépendance — d3-hierarchy, déjà présent.
 *
 * Mutualisation : deux entités aux proportions voisines partagent la même image.
 * Les parts sont donc arrondies par paliers avant de composer la clé de cache,
 * ce qui borne la mémoire (une image 64×64 en @2x pèse ~64 Ko).
 */
import { treemap, hierarchy } from "d3-hierarchy";

const DPR = 2;                 // vignettes @2x : nettes sur écran dense
const STEP = 4;                // arrondi des parts, en % (mutualisation du cache)

/** Clé de cache : même dessin ⇒ même clé ⇒ une seule image en mémoire. */
export function spriteKey(kind, parts, colors, px) {
  const q = parts.map(p => Math.round(p * 100 / STEP) * STEP).join("-");
  // Les couleurs entrent dans la clé : sans elles, changer une teinte à la main
  // laisserait map.hasImage() renvoyer vrai et l'ancienne vignette à l'écran.
  return `chart_${kind}_${px}_${q}_${colors.join("").replace(/#/g, "")}`;
}

function ring(ctx, s, parts, colors, inner) {
  const cx = s / 2, cy = s / 2, r = s / 2 - 2;
  let a0 = -Math.PI / 2;
  parts.forEach((p, i) => {
    if (p <= 0) return;
    const a1 = a0 + p * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, r, a0, a1);
    ctx.closePath();
    ctx.fillStyle = colors[i % colors.length];
    ctx.fill();
    a0 = a1;
  });
  // Liseré blanc : indispensable pour détacher la vignette d'un fond satellite
  ctx.lineWidth = 1.5;
  ctx.strokeStyle = "rgba(255,255,255,.95)";
  ctx.beginPath(); ctx.arc(cx, cy, r, 0, Math.PI * 2); ctx.stroke();
  if (inner > 0) {
    ctx.globalCompositeOperation = "destination-out";
    ctx.beginPath(); ctx.arc(cx, cy, r * inner, 0, Math.PI * 2); ctx.fill();
    ctx.globalCompositeOperation = "source-over";
  }
}

function bars(ctx, s, vals, colors, stacked) {
  const pad = 2, w = s - pad * 2, h = s - pad * 2;
  if (stacked) {
    // Barre unique : la hauteur totale porte le volume, les segments la composition
    const tot = vals.reduce((a, b) => a + b, 0) || 1;
    const bw = w * 0.55, x = (s - bw) / 2;
    let y = s - pad;
    vals.forEach((v, i) => {
      const seg = (v / tot) * h;
      ctx.fillStyle = colors[i % colors.length];
      ctx.fillRect(x, y - seg, bw, seg);
      y -= seg;
    });
    ctx.lineWidth = 1.2; ctx.strokeStyle = "rgba(255,255,255,.95)";
    ctx.strokeRect(x, pad, bw, h);
  } else {
    // Valeurs DÉJÀ normalisées sur le maximum global de la couche par l'appelant.
    // Les renormaliser ici sur le maximum local remettrait chaque entité à sa
    // propre échelle : la plus haute barre atteindrait le sommet partout, et
    // toutes les vignettes se ressembleraient — le graphique ne dirait plus rien.
    const bw = w / vals.length;
    vals.forEach((v, i) => {
      const bh = Math.max(1, Math.min(1, v) * h);
      ctx.fillStyle = colors[i % colors.length];
      ctx.fillRect(pad + i * bw + bw * 0.12, s - pad - bh, bw * 0.76, bh);
    });
    ctx.lineWidth = 1.2; ctx.strokeStyle = "rgba(255,255,255,.95)";
    ctx.beginPath(); ctx.moveTo(pad, s - pad); ctx.lineTo(s - pad, s - pad); ctx.stroke();
  }
}

function tmap(ctx, s, vals, colors) {
  const root = hierarchy({ children: vals.map((v, i) => ({ v: Math.max(v, 0), i })) })
    .sum(d => d.v);
  treemap().size([s - 3, s - 3]).paddingInner(1).round(true)(root);
  for (const n of root.leaves()) {
    ctx.fillStyle = colors[n.data.i % colors.length];
    ctx.fillRect(1.5 + n.x0, 1.5 + n.y0, n.x1 - n.x0, n.y1 - n.y0);
  }
  ctx.lineWidth = 1.5; ctx.strokeStyle = "rgba(255,255,255,.95)";
  ctx.strokeRect(1.5, 1.5, s - 3, s - 3);
}

/**
 * Dessine une vignette et renvoie un ImageData prêt pour map.addImage().
 * @param kind  "pie" | "donut" | "bars" | "stacked" | "treemap"
 * @param vals  valeurs brutes (déjà normalisées par l'appelant pour "bars")
 */
export function renderSprite(kind, vals, colors, px) {
  const s = px, cv = document.createElement("canvas");
  cv.width = cv.height = s * DPR;
  const ctx = cv.getContext("2d");
  ctx.scale(DPR, DPR);
  ctx.clearRect(0, 0, s, s);

  const tot = vals.reduce((a, b) => a + Math.max(b, 0), 0);
  const parts = tot > 0 ? vals.map(v => Math.max(v, 0) / tot) : vals.map(() => 0);

  if (kind === "pie")          ring(ctx, s, parts, colors, 0);
  else if (kind === "donut")   ring(ctx, s, parts, colors, 0.52);
  else if (kind === "stacked") bars(ctx, s, vals, colors, true);
  else if (kind === "treemap") tmap(ctx, s, vals, colors);
  else                         bars(ctx, s, vals, colors, false);

  return ctx.getImageData(0, 0, cv.width, cv.height);
}

/** Facteur de taille par entité : l'AIRE doit être proportionnelle, donc √. */
export function sizeFactor(total, maxTotal, min = 0.45) {
  if (!maxTotal || maxTotal <= 0 || !(total > 0)) return min;
  return Math.max(min, Math.min(1, Math.sqrt(total / maxTotal)));
}

/**
 * Couleurs effectives d'un graphique : palette, éventuellement inversée, puis
 * remplacements manuels par position. Centralisé ici pour que la carte, la
 * légende et le panneau de réglage ne puissent pas diverger.
 */
export function resolveChartColors(cfg, ramps, n) {
  const base = ramps[cfg?.palette] || ramps.categorial || ["#888"];
  const arr = cfg?.invert ? [...base].reverse() : base;
  const out = [];
  for (let i = 0; i < Math.max(n, 1); i++) {
    out.push(cfg?.colors?.[i] || arr[i % arr.length]);
  }
  return out;
}

export const DEFAULT_PX = { pie: 64, donut: 64, bars: 60, stacked: 52, treemap: 80 };
export const CHART_KINDS = [
  { id: "pie",     label: "Camembert" },
  { id: "donut",   label: "Anneau" },
  { id: "bars",    label: "Barres" },
  { id: "stacked", label: "Empilées" },
  { id: "treemap", label: "Treemap" },
];
