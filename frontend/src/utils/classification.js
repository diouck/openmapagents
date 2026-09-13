import { RAMPS, BIVARIATE_PALETTES } from "../config";

// ── Style BIVARIÉ (2 variables croisées en une matrice 3×3) ──────────────────
function tertiles(vals) {
  const s = [...vals].sort((a, b) => a - b);
  const q = (p) => s[Math.min(s.length - 1, Math.max(0, Math.floor(p * s.length)))];
  let b1 = q(1 / 3), b2 = q(2 / 3);
  if (b2 <= b1) b2 = b1 + (Math.abs(b1) || 1) * 1e-4 + 1e-6;   // bornes strictement croissantes (step MapLibre)
  return [b1, b2];
}
export function buildBivariate(layer, cfg) {
  if (!cfg || !cfg.attrX || !cfg.attrY) return null;
  const vx = getNumVals(layer, cfg.attrX), vy = getNumVals(layer, cfg.attrY);
  if (!vx.length || !vy.length) return null;
  const [bx1, bx2] = tertiles(vx), [by1, by2] = tertiles(vy);
  const palette = BIVARIATE_PALETTES[cfg.palette] || Object.values(BIVARIATE_PALETTES)[0];
  const classX = ["step", ["to-number", ["get", cfg.attrX], 0], 0, bx1, 1, bx2, 2];
  const classY = ["step", ["to-number", ["get", cfg.attrY], 0], 0, by1, 1, by2, 2];
  const idx = ["+", ["*", classX, 3], classY];
  const expr = ["match", idx];
  for (let i = 0; i < 9; i++) { expr.push(i); expr.push(palette[i]); }
  expr.push("#cccccc");
  return {
    type: "bivariate", palette, expression: expr,
    attrX: cfg.attrX, attrY: cfg.attrY, paletteKey: cfg.palette,
    label_a: cfg.attrX, label_b: cfg.attrY, levels: ["Faible", "Moyen", "Élevé"],
    breaksX: [bx1, bx2], breaksY: [by1, by2],
  };
}

export function getLayerAttrs(layer) {
  const feats = layer.geojson?.features || [];
  if (!feats.length) return { num: [], cat: [] };
  const sample = feats.slice(0, 100);
  const keys = new Set();
  sample.forEach(f => Object.keys(f.properties || {}).forEach(k => keys.add(k)));
  const num = [], cat = [];
  keys.forEach(k => {
    if (["id", "geom_json", "geom_wkt"].includes(k)) return;
    const vals = sample.map(f => f.properties?.[k]).filter(v => v != null && v !== "" && v !== "None");
    if (!vals.length) return;
    const nv = vals.filter(v => typeof v === "number" || (typeof v === "string" && !isNaN(Number(v)) && v.trim() !== ""));
    if (nv.length > vals.length * 0.6) num.push(k);
    else {
      const u = new Set(vals.map(String));
      if (u.size <= 50 && u.size >= 2) cat.push(k);
    }
  });
  return { num, cat };
}

export function getNumVals(layer, attr) {
  return (layer.geojson?.features || [])
    .map(f => f.properties?.[attr])
    .filter(v => v != null && v !== "" && v !== "None")  // exclure null/undefined AVANT conversion
    .map(v => typeof v === "number" ? v : Number(v))
    .filter(v => !isNaN(v) && isFinite(v));
}

export function getUniques(layer, attr) {
  const c = {};
  (layer.geojson?.features || []).forEach(f => {
    const v = f.properties?.[attr];
    if (v != null && v !== "" && v !== "None") { const s = String(v); c[s] = (c[s] || 0) + 1; }
  });
  return Object.entries(c).sort((a, b) => b[1] - a[1]).map(([value, count]) => ({ value, count }));
}

function classifyQuantile(vals, n) {
  const s = [...vals].sort((a, b) => a - b);
  const br = [s[0]];
  for (let i = 1; i < n; i++) br.push(s[Math.max(0, Math.round(i / n * s.length) - 1)]);
  br.push(s[s.length - 1]);
  return [...new Set(br)];
}

function classifyEqual(vals, n) {
  const mn = Math.min(...vals), mx = Math.max(...vals), st = (mx - mn) / n;
  const br = [];
  for (let i = 0; i <= n; i++) br.push(Math.round((mn + st * i) * 100) / 100);
  return br;
}

function classifyJenks(vals, n) {
  const s = [...vals].sort((a, b) => a - b), len = s.length;
  if (len <= n) return classifyEqual(vals, n);
  const m1 = Array.from({ length: len + 1 }, () => new Float64Array(n + 1).fill(Infinity));
  const m2 = Array.from({ length: len + 1 }, () => new Int32Array(n + 1));
  for (let i = 1; i <= n; i++) m1[1][i] = 0;
  for (let i = 2; i <= len; i++) m1[i][1] = 0;
  for (let l = 2; l <= len; l++) {
    let sum = 0, sq = 0;
    for (let m = 1; m <= l; m++) {
      sum += s[l - m]; sq += s[l - m] ** 2;
      const v = sq - sum * sum / m;
      if (m === l) { m1[l][1] = v; m2[l][1] = 1; continue; }
      for (let j = 2; j <= n; j++) {
        const t = v + m1[l - m][j - 1];
        if (t < m1[l][j]) { m1[l][j] = t; m2[l][j] = l - m + 1; }
      }
    }
  }
  const br = [s[0]]; let k = len;
  for (let j = n; j >= 2; j--) { br.unshift(s[Math.min(m2[k][j] - 1, len - 1)]); k = m2[k][j] - 1; }
  br.push(s[len - 1]);
  return [...new Set(br)].sort((a, b) => a - b);
}

// Centroïde simple (moyenne des coordonnées) — suffisant pour placer un rond.
function simpleCentroid(geom) {
  let sx = 0, sy = 0, n = 0;
  const walk = (c) => { if (typeof c[0] === "number") { sx += c[0]; sy += c[1]; n++; return; } for (const k of c) walk(k); };
  if (geom?.coordinates) walk(geom.coordinates);
  return n ? [sx / n, sy / n] : null;
}

// Taille proportionnelle INDÉPENDANTE (2e style combinable avec une couleur graduée/
// catégorisée). mode "radius" (points/polygones) ou "width" (lignes). → expression MapLibre.
// Pour les polygones : on génère des ronds proportionnels placés au centroïde (bubbles).
export function buildSize(layer, cfg) {
  if (!cfg || !cfg.attribute) return null;
  const vals = getNumVals(layer, cfg.attribute);
  if (!vals.length) return null;
  const minVal = Math.min(...vals), maxVal = Math.max(...vals);
  const mode = cfg.mode === "width" ? "width" : "radius";
  const minSize = cfg.minSize ?? (mode === "width" ? 1 : 3);
  const maxSize = cfg.maxSize ?? (mode === "width" ? 12 : 26);
  const expr = ["interpolate", ["linear"], ["to-number", ["get", cfg.attribute], 0],
    minVal, minSize, maxVal === minVal ? maxVal + 1 : maxVal, maxSize];

  const gt = (layer.geojson?.features || []).find(f => f?.geometry)?.geometry?.type || "";
  const isPoly = /Polygon/i.test(gt);
  let bubbles = null;
  if (mode === "radius" && isPoly) {
    bubbles = {
      type: "FeatureCollection",
      features: (layer.geojson.features || []).map(f => {
        const c = simpleCentroid(f.geometry);
        return c ? { type: "Feature", geometry: { type: "Point", coordinates: c }, properties: f.properties || {} } : null;
      }).filter(Boolean),
    };
  }
  return {
    attribute: cfg.attribute, mode, minVal, maxVal, minSize, maxSize,
    radiusExpression: mode === "radius" ? expr : null,
    widthExpression:  mode === "width"  ? expr : null,
    bubbles,
  };
}

export function buildClassification(layer, cfg) {
  if (!cfg || cfg.type === "none") return null;
  const { type, attribute, method, nClasses, ramp, customBreaks, invertRamp } = cfg;

  if (type === "categorized") {
    const u = getUniques(layer, attribute);
    const baseCols = RAMPS[ramp] || RAMPS.categorial;
    const cols = cfg.invertRamp ? [...baseCols].reverse() : baseCols;
    const man = cfg.manual?.type === "categorized" ? cfg.manual : null;
    let entries;
    if (man && Array.isArray(man.entries) && man.entries.length) {
      // Édition manuelle : la liste éditée fait autorité (couleur/étiquette/suppression)
      const countOf = {};
      u.forEach(v => { countOf[String(v.value)] = v.count; });
      entries = man.entries.map(e => ({
        value: e.value, color: e.color, label: e.label ?? undefined, count: countOf[String(e.value)] ?? 0,
      }));
    } else {
      entries = u.slice(0, cols.length).map((v, i) => ({
        value: v.value, color: cols[i % cols.length], count: v.count,
      }));
    }
    const expr = ["match", ["to-string", ["get", attribute]]];
    entries.forEach(e => { expr.push(e.value); expr.push(e.color); });
    expr.push("#888");
    return { type: "categorized", attribute, entries, expression: expr,
             manual: !!man, invertRamp: !!cfg.invertRamp };
  }

  // ── Icône / Emoji ────────────────────────────────────────────
  if (type === "symbol") {
    return {
      type: "symbol",
      symbolMode:  cfg.symbolMode  || "emoji",
      emoji:       cfg.emoji       || "📍",
      emojiSize:   cfg.emojiSize   || 20,
      customImage: cfg.customImage || null,
      imageSize:   cfg.imageSize   || 1,
      expression:  null, // pas d'expression couleur
    };
  }

  // ── Symboles proportionnels (cercles) ────────────────────────
  if (type === "proportional") {
    const vals = getNumVals(layer, attribute);
    if (!vals.length) return null;
    const minVal = Math.min(...vals);
    const maxVal = Math.max(...vals);
    const minSize = cfg.minSize ?? 3;
    const maxSize = cfg.maxSize ?? 30;
    // Expression MapLibre : interpolate linéaire entre minVal→minSize et maxVal→maxSize
    const expr = [
      "interpolate", ["linear"],
      ["to-number", ["get", attribute], 0],
      minVal, minSize,
      maxVal, maxSize,
    ];
    // Couleur : le panneau propose une palette pour les symboles proportionnels
    // (et l'aperçu montre 3 cercles de teintes différentes) — on construit donc
    // bien une expression couleur graduée sur le même attribut, sinon la palette
    // choisie était ignorée et les cercles restaient de la couleur de la couche.
    const colorCr = buildClassification(layer, { ...cfg, type: "graduated" });
    return {
      type: "proportional",
      attribute,
      minVal, maxVal, minSize, maxSize,
      radiusExpression: expr,
      expression: colorCr?.expression || null,
      classes:    colorCr?.classes    || [],
      breaks:     colorCr?.breaks     || [],
      ramp, invertRamp: !!cfg.invertRamp,
    };
  }

  // ── Traits proportionnels (line-width) ────────────────────────
  if (type === "proportional_line") {
    const vals = getNumVals(layer, attribute);
    if (!vals.length) return null;
    const minVal = Math.min(...vals);
    const maxVal = Math.max(...vals);
    const minSize = cfg.minSize ?? 1;
    const maxSize = cfg.maxSize ?? 12;
    const expr = [
      "interpolate", ["linear"],
      ["to-number", ["get", attribute], 0],
      minVal, minSize,
      maxVal, maxSize,
    ];
    return {
      type: "proportional_line",
      attribute,
      minVal, maxVal, minSize, maxSize,
      widthExpression: expr,
      expression: null,
    };
  }

  if (type === "graduated") {
    const vals = getNumVals(layer, attribute);
    if (!vals.length) return null;
    const nc = nClasses || 5;

    // ── Édition manuelle : bornes / couleurs / étiquettes imposées ──
    const man = cfg.manual?.type === "graduated" ? cfg.manual : null;
    if (man && Array.isArray(man.breaks) && man.breaks.length >= 2) {
      const mbr = man.breaks;
      const classes = [];
      for (let i = 0; i < mbr.length - 1; i++) {
        const count = vals.filter(v => v >= mbr[i] && (i === mbr.length - 2 ? v <= mbr[i + 1] : v < mbr[i + 1])).length;
        classes.push({
          min: mbr[i], max: mbr[i + 1],
          color: man.colors?.[i] || "#888",
          label: man.labels?.[i] ?? undefined,
          count,
        });
      }
      const expr = ["step", ["to-number", ["get", attribute], 0]];
      expr.push(classes[0]?.color || "#888");
      let lastStop = -Infinity;
      classes.forEach((c, i) => {
        if (i > 0 && c.min > lastStop) { expr.push(c.min); expr.push(c.color); lastStop = c.min; }
      });
      return { type: "graduated", attribute, method, classes, breaks: mbr, expression: expr,
               manual: true, invertRamp: !!cfg.invertRamp };
    }

    let br;
    switch (method) {
      case "quantile": br = classifyQuantile(vals, nc); break;
      case "jenks":    br = classifyJenks(vals, nc);    break;
      case "equal":    br = classifyEqual(vals, nc);    break;
      case "fixed":    br = customBreaks || classifyEqual(vals, nc); break;
      default:         br = classifyQuantile(vals, nc);
    }
    // Inverser la palette si demandé (cfg.invertRamp === true)
    const baseCols = RAMPS[ramp] || RAMPS.viridis;
    const cols = cfg.invertRamp ? [...baseCols].reverse() : baseCols;
    const classes = [];
    for (let i = 0; i < br.length - 1; i++) {
      const count = vals.filter(v => v >= br[i] && (i === br.length - 2 ? v <= br[i + 1] : v < br[i + 1])).length;
      classes.push({
        min: br[i], max: br[i + 1],
        color: cols[Math.round(i / (br.length - 2) * (cols.length - 1))],
        count,
      });
    }
    // MapLibre exige des bornes STRICTEMENT croissantes dans un `step` : sur des
    // données peu variées, deux ruptures peuvent être égales → on saute les
    // doublons, sinon l'expression est rejetée et le style ne s'applique pas.
    const expr = ["step", ["to-number", ["get", attribute], 0]];
    expr.push(classes[0]?.color || "#888");
    let lastStop = -Infinity;
    classes.forEach((c, i) => {
      if (i > 0 && c.min > lastStop) { expr.push(c.min); expr.push(c.color); lastStop = c.min; }
    });
    return { type: "graduated", attribute, method, classes, breaks: br, expression: expr,
             invertRamp: !!cfg.invertRamp };
  }
  return null;
}
