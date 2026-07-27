/**
 * fieldCalc.js — Calculateur de champ : évalue une expression par entité.
 *
 * PAS d'eval / new Function, pour trois raisons :
 *   • les couches sont sauvegardées et partagées — une expression mémorisée puis
 *     rejouée chez un tiers serait une exécution de code arbitraire ;
 *   • `new Function` est bloqué par toute CSP stricte ;
 *   • additionner deux colonnes n'a aucun besoin d'accéder à window ou fetch.
 *
 * Analyse en trois temps : lexèmes → notation postfixée (shunting-yard) → pile
 * d'évaluation. Le moteur ne connaît que des nombres, des noms de champs et des
 * opérateurs : il ne peut rien atteindre d'autre.
 */
import area from "@turf/area";
import length from "@turf/length";
import centroid from "@turf/centroid";

// ── Vocabulaire ──────────────────────────────────────────────────────────────

// Fonctions scalaires : nom → [arité min, arité max, implémentation]
const FUNCS = {
  ABS:       [1, 1, a => Math.abs(a[0])],
  ARRONDI:   [1, 2, a => { const p = 10 ** (a[1] ?? 0); return Math.round(a[0] * p) / p; }],
  MIN:       [2, 9, a => Math.min(...a)],
  MAX:       [2, 9, a => Math.max(...a)],
  RACINE:    [1, 1, a => Math.sqrt(a[0])],
  LOG:       [1, 1, a => Math.log10(a[0])],
  LN:        [1, 1, a => Math.log(a[0])],
  EXP:       [1, 1, a => Math.exp(a[0])],
  PUISSANCE: [2, 2, a => a[0] ** a[1]],
  SI:        [3, 3, a => (a[0] ? a[1] : a[2])],
};

// Agrégats sur TOUTE la couche : leur argument est un NOM de champ, pas une
// valeur. Ils sont donc résolus avant l'analyse, et remplacés par une constante.
const AGGS = {
  SOMME:   vs => vs.reduce((x, y) => x + y, 0),
  MOYENNE: vs => (vs.length ? vs.reduce((x, y) => x + y, 0) / vs.length : NaN),
  MINIMUM: vs => (vs.length ? Math.min(...vs) : NaN),
  MAXIMUM: vs => (vs.length ? Math.max(...vs) : NaN),
  NOMBRE:  vs => vs.length,
};

export const GEO_VARS = ["$aire_m2", "$aire_ha", "$aire_km2", "$perimetre_m", "$longueur_m", "$x", "$y"];
export const FUNC_NAMES = Object.keys(FUNCS);
export const AGG_NAMES = Object.keys(AGGS);
export const OPERATORS = ["+", "-", "*", "/", "%", "^", "(", ")", ",", "<", "<=", ">", ">=", "=", "<>"];

const OPS = {
  "<": [1, "L"], "<=": [1, "L"], ">": [1, "L"], ">=": [1, "L"], "=": [1, "L"], "<>": [1, "L"],
  "+": [2, "L"], "-": [2, "L"],
  "*": [3, "L"], "/": [3, "L"], "%": [3, "L"],
  "^": [4, "R"],
  "u-": [5, "R"],                 // moins unaire
};

// ── Lexèmes ──────────────────────────────────────────────────────────────────

function tokenize(src) {
  const t = [];
  let i = 0;
  const s = String(src || "");
  while (i < s.length) {
    const c = s[i];
    if (/\s/.test(c)) { i++; continue; }

    // Nom de champ entre guillemets : autorise espaces et accents
    if (c === '"') {
      const j = s.indexOf('"', i + 1);
      if (j < 0) throw new Error("Guillemet non fermé.");
      t.push({ k: "field", v: s.slice(i + 1, j) });
      i = j + 1; continue;
    }
    if (/[0-9]/.test(c) || (c === "." && /[0-9]/.test(s[i + 1] || ""))) {
      let j = i;
      while (j < s.length && /[0-9.]/.test(s[j])) j++;
      t.push({ k: "num", v: parseFloat(s.slice(i, j)) });
      i = j; continue;
    }
    if (c === "$" || /[A-Za-zÀ-ÿ_]/.test(c)) {
      let j = i;
      while (j < s.length && /[A-Za-zÀ-ÿ0-9_$.]/.test(s[j])) j++;
      const w = s.slice(i, j);
      const up = w.toUpperCase();
      if (FUNCS[up])      t.push({ k: "func", v: up });
      else if (AGGS[up])  t.push({ k: "agg",  v: up });
      else                t.push({ k: "field", v: w });
      i = j; continue;
    }
    const two = s.slice(i, i + 2);
    if (two === "<=" || two === ">=" || two === "<>") { t.push({ k: "op", v: two }); i += 2; continue; }
    if ("+-*/%^(),<>=".includes(c)) { t.push({ k: "op", v: c }); i++; continue; }
    throw new Error(`Caractère non reconnu : « ${c} »`);
  }
  return t;
}

/**
 * Remplace `AGG(champ)` par la constante correspondante, calculée sur la couche.
 * Fait avant l'analyse : l'argument d'un agrégat est un nom, pas une valeur, et
 * mélanger les deux compliquerait inutilement l'évaluateur.
 */
function resolveAggregates(tokens, features) {
  const out = [];
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i];
    if (t.k !== "agg") { out.push(t); continue; }
    if (tokens[i + 1]?.v !== "(" || tokens[i + 2]?.k !== "field" || tokens[i + 3]?.v !== ")")
      throw new Error(`${t.v} attend un nom de champ, par exemple ${t.v}(population).`);
    const name = tokens[i + 2].v;
    const vals = [];
    for (const f of features) {
      const n = Number(f?.properties?.[name]);
      if (Number.isFinite(n)) vals.push(n);
    }
    out.push({ k: "num", v: AGGS[t.v](vals) });
    i += 3;
  }
  return out;
}

// ── Shunting-yard → notation postfixée ───────────────────────────────────────

function toRPN(tokens) {
  const out = [], stack = [];
  // Nombre d'arguments de chaque appel de fonction en cours. Sans ce comptage,
  // l'évaluateur devrait deviner l'arité en dépilant, et « 1 + MIN(2,3) »
  // verrait MIN absorber le 1.
  const argc = [];
  let prev = null;
  for (const t of tokens) {
    if (t.k === "num" || t.k === "field") out.push(t);
    else if (t.k === "func") stack.push(t);
    else if (t.v === ",") {
      while (stack.length && stack[stack.length - 1].v !== "(") out.push(stack.pop());
      if (!stack.length || !argc.length) throw new Error("Virgule hors d'une fonction.");
      argc[argc.length - 1]++;
    } else if (t.v === "(") {
      // Parenthèse d'appel si elle suit immédiatement un nom de fonction
      const isCall = stack.length && stack[stack.length - 1].k === "func";
      stack.push({ k: "op", v: "(", call: isCall });
      if (isCall) argc.push(1);
    } else if (t.v === ")") {
      while (stack.length && stack[stack.length - 1].v !== "(") out.push(stack.pop());
      if (!stack.length) throw new Error("Parenthèse fermante en trop.");
      const par = stack.pop();
      if (par.call) {
        const n = argc.pop();
        const f = stack.pop();
        const [amin, amax] = FUNCS[f.v];
        if (n < amin || n > amax)
          throw new Error(`${f.v} attend ${amin === amax ? amin : `${amin} à ${amax}`} argument(s), ${n} fourni(s).`);
        out.push({ ...f, n });
      }
    } else {
      // Moins unaire : en début d'expression, après un opérateur ou après « ( »
      const unary = t.v === "-" && (!prev || (prev.k === "op" && prev.v !== ")"));
      const op = unary ? "u-" : t.v;
      const [p, assoc] = OPS[op] || [];
      if (p === undefined) throw new Error(`Opérateur inconnu : ${t.v}`);
      while (stack.length) {
        const top = stack[stack.length - 1];
        if (top.k === "func") { out.push(stack.pop()); continue; }
        const tp = OPS[top.v]?.[0];
        if (tp === undefined) break;
        if (tp > p || (tp === p && assoc === "L")) out.push(stack.pop());
        else break;
      }
      stack.push({ k: "op", v: op });
    }
    prev = t;
  }
  while (stack.length) {
    const top = stack.pop();
    if (top.v === "(") throw new Error("Parenthèse ouvrante non fermée.");
    out.push(top);
  }
  return out;
}

// ── Évaluation ───────────────────────────────────────────────────────────────

function geoValue(name, feature) {
  try {
    if (name === "$aire_m2")     return area(feature);
    if (name === "$aire_ha")     return area(feature) / 1e4;
    if (name === "$aire_km2")    return area(feature) / 1e6;
    if (name === "$perimetre_m") return length(feature, { units: "kilometers" }) * 1000;
    if (name === "$longueur_m")  return length(feature, { units: "kilometers" }) * 1000;
    if (name === "$x")           return centroid(feature).geometry.coordinates[0];
    if (name === "$y")           return centroid(feature).geometry.coordinates[1];
  } catch (_) { /* géométrie inexploitable */ }
  return NaN;
}

function evalRPN(rpn, feature) {
  const st = [];
  for (const t of rpn) {
    if (t.k === "num") { st.push(t.v); continue; }
    if (t.k === "field") {
      if (t.v.startsWith("$")) { st.push(geoValue(t.v, feature)); continue; }
      const raw = feature?.properties?.[t.v];
      const n = typeof raw === "number" ? raw : Number(String(raw ?? "").replace(",", "."));
      st.push(Number.isFinite(n) ? n : NaN);
      continue;
    }
    if (t.k === "func") {
      const fn = FUNCS[t.v][2];
      if (st.length < t.n) throw new Error(`Arguments manquants pour ${t.v}.`);
      const args = [];
      for (let i = 0; i < t.n; i++) args.unshift(st.pop());   // arité fixée à l'analyse
      st.push(fn(args));
      continue;
    }
    if (t.v === "u-") {
      if (!st.length) throw new Error("Il manque une valeur après « - ».");
      st.push(-st.pop()); continue;
    }
    // Dépiler une pile vide rend `undefined` en JS, et l'expression donnerait
    // silencieusement NaN : on préfère une erreur explicite.
    if (st.length < 2) throw new Error(`Il manque une valeur autour de « ${t.v} ».`);
    const b = st.pop(), a = st.pop();
    switch (t.v) {
      case "+": st.push(a + b); break;
      case "-": st.push(a - b); break;
      case "*": st.push(a * b); break;
      case "/": st.push(b === 0 ? NaN : a / b); break;   // division par zéro → non calculable
      case "%": st.push(b === 0 ? NaN : a % b); break;
      case "^": st.push(a ** b); break;
      case "<": st.push(a < b ? 1 : 0); break;
      case "<=": st.push(a <= b ? 1 : 0); break;
      case ">": st.push(a > b ? 1 : 0); break;
      case ">=": st.push(a >= b ? 1 : 0); break;
      case "=": st.push(a === b ? 1 : 0); break;
      case "<>": st.push(a !== b ? 1 : 0); break;
      default: throw new Error(`Opérateur non géré : ${t.v}`);
    }
  }
  if (st.length !== 1) throw new Error("Expression incomplète.");
  return st[0];
}

/** Compile une expression pour une couche donnée (agrégats résolus une fois). */
export function compile(expr, features) {
  if (!String(expr || "").trim()) throw new Error("Expression vide.");
  const rpn = toRPN(resolveAggregates(tokenize(expr), features || []));
  return (feature) => {
    const v = evalRPN(rpn, feature);
    return Number.isFinite(v) ? v : null;
  };
}

/** Aperçu : premiers résultats, nombre d'échecs, étendue des valeurs. */
export function preview(expr, features, n = 5) {
  const fn = compile(expr, features);
  const sample = [], all = [];
  let failed = 0;
  features.forEach((f, i) => {
    const v = fn(f);
    if (v === null) failed++; else all.push(v);
    if (i < n) sample.push(v);
  });
  return {
    sample, failed, total: features.length,
    min: all.length ? Math.min(...all) : null,
    max: all.length ? Math.max(...all) : null,
  };
}

/** Applique l'expression et retourne un NOUVEAU GeoJSON enrichi. */
export function applyField(geojson, expr, name) {
  const feats = geojson?.features || [];
  const fn = compile(expr, feats);
  return {
    type: "FeatureCollection",
    features: feats.map(f => ({ ...f, properties: { ...(f.properties || {}), [name]: fn(f) } })),
  };
}
