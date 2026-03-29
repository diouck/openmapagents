import { RAMPS } from "../config";

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
    .map(f => { const v = f.properties?.[attr]; return typeof v === "number" ? v : Number(v); })
    .filter(v => !isNaN(v));
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

export function buildClassification(layer, cfg) {
  if (!cfg || cfg.type === "none") return null;
  const { type, attribute, method, nClasses, ramp, customBreaks } = cfg;

  if (type === "categorized") {
    const u = getUniques(layer, attribute);
    const cols = RAMPS[ramp] || RAMPS.categorial;
    const entries = u.slice(0, cols.length).map((v, i) => ({
      value: v.value, color: cols[i % cols.length], count: v.count,
    }));
    const expr = ["match", ["to-string", ["get", attribute]]];
    entries.forEach(e => { expr.push(e.value); expr.push(e.color); });
    expr.push("#888");
    return { type: "categorized", attribute, entries, expression: expr };
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
    return {
      type: "proportional",
      attribute,
      minVal, maxVal, minSize, maxSize,
      radiusExpression: expr,
      // Pas d'expression couleur — la couleur de base reste
      expression: null,
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
    let br;
    switch (method) {
      case "quantile": br = classifyQuantile(vals, nc); break;
      case "jenks": br = classifyJenks(vals, nc); break;
      case "equal": br = classifyEqual(vals, nc); break;
      case "fixed": br = customBreaks || classifyEqual(vals, nc); break;
      default: br = classifyQuantile(vals, nc);
    }
    const cols = RAMPS[ramp] || RAMPS.viridis;
    const classes = [];
    for (let i = 0; i < br.length - 1; i++) {
      const count = vals.filter(v => v >= br[i] && (i === br.length - 2 ? v <= br[i + 1] : v < br[i + 1])).length;
      classes.push({
        min: br[i], max: br[i + 1],
        color: cols[Math.round(i / (br.length - 2) * (cols.length - 1))],
        count,
      });
    }
    const expr = ["step", ["to-number", ["get", attribute], 0]];
    expr.push(classes[0]?.color || "#888");
    classes.forEach((c, i) => { if (i > 0) { expr.push(c.min); expr.push(c.color); } });
    return { type: "graduated", attribute, method, classes, breaks: br, expression: expr };
  }
  return null;
}
