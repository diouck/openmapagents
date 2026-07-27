/**
 * tableJoin.js — Jointure attributaire d'un tableau (CSV) sur une couche vecteur.
 *
 * Une jointure échoue presque toujours pour la même raison : les clés ne se
 * correspondent pas *littéralement* alors qu'elles désignent la même chose.
 * Codes INSEE dont Excel a mangé le zéro initial (« 01004 » → « 1004 »), casse
 * et accents divergents, espaces insécables, nombre d'un côté et chaîne de
 * l'autre. Ce module traite donc la normalisation et le DIAGNOSTIC comme le
 * cœur du sujet, la copie des colonnes n'étant que la dernière étape.
 */

// ── Lecture CSV ──────────────────────────────────────────────────────────────

/** Devine le séparateur d'après la ligne d'en-tête (hors guillemets). */
function sniffDelimiter(headerLine) {
  const cands = [";", ",", "\t", "|"];
  let best = ",", bestN = 0;
  for (const d of cands) {
    let n = 0, inQ = false;
    for (let i = 0; i < headerLine.length; i++) {
      const c = headerLine[i];
      if (c === '"') inQ = !inQ;
      else if (c === d && !inQ) n++;
    }
    if (n > bestN) { bestN = n; best = d; }
  }
  return best;
}

/**
 * Analyse un CSV en { columns, rows, delimiter }.
 * Gère les guillemets, les séparateurs à l'intérieur des champs, les guillemets
 * doublés, les fins de ligne CRLF et le BOM UTF-8.
 */
export function parseCSV(text) {
  let s = String(text || "").replace(/^﻿/, "");   // BOM
  if (!s.trim()) return { columns: [], rows: [], delimiter: "," };

  const firstEnd = s.search(/\r?\n/);
  const delimiter = sniffDelimiter(firstEnd > 0 ? s.slice(0, firstEnd) : s);

  const rows = [];
  let field = "", row = [], inQ = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (inQ) {
      if (c === '"') {
        if (s[i + 1] === '"') { field += '"'; i++; }   // guillemet échappé
        else inQ = false;
      } else field += c;
    } else if (c === '"') inQ = true;
    else if (c === delimiter) { row.push(field); field = ""; }
    else if (c === "\n") { row.push(field); rows.push(row); row = []; field = ""; }
    else if (c !== "\r") field += c;
  }
  if (field.length || row.length) { row.push(field); rows.push(row); }
  if (!rows.length) return { columns: [], rows: [], delimiter };

  const header = rows.shift().map((h, i) => (h.trim() || `colonne_${i + 1}`));
  const out = rows
    .filter(r => r.some(v => String(v).trim() !== ""))    // ignore les lignes vides
    .map(r => Object.fromEntries(header.map((h, i) => [h, (r[i] ?? "").trim()])));
  return { columns: header, rows: out, delimiter };
}

// ── Normalisation des clés ───────────────────────────────────────────────────

/**
 * @param opts.trim        espaces de bord (toujours souhaitable)
 * @param opts.caseless    ignorer la casse
 * @param opts.noAccents   replier les accents
 * @param opts.pad         longueur de remplissage par zéros à gauche (0 = off)
 */
export function normKey(v, opts = {}) {
  let s = v == null ? "" : String(v);
  if (opts.trim !== false) s = s.trim().replace(/ /g, "");   // espace insécable
  if (opts.noAccents) s = s.normalize("NFD").replace(/[̀-ͯ]/g, "");
  if (opts.caseless) s = s.toLowerCase();
  // Excel transforme « 01004 » en nombre et perd le zéro : on peut le rétablir.
  if (opts.pad > 0 && /^\d+$/.test(s)) s = s.padStart(opts.pad, "0");
  return s;
}

/** Convertit en nombre les valeurs qui en sont un (virgule décimale admise). */
function coerce(v) {
  if (typeof v !== "string") return v;
  const t = v.trim();
  if (t === "") return null;
  const n = Number(t.replace(",", ".").replace(/\s/g, ""));
  return Number.isFinite(n) && /^[-+]?[\d\s.,]+$/.test(t) ? n : v;
}

// ── Diagnostic + jointure ────────────────────────────────────────────────────

/**
 * Compare les deux jeux de clés SANS rien modifier : c'est ce qu'on montre à
 * l'utilisateur avant qu'il ne valide.
 */
export function analyzeJoin(features, geoKey, rows, csvKey, opts = {}) {
  const index = new Map();
  let dupes = 0;
  for (const r of rows) {
    const k = normKey(r[csvKey], opts);
    if (!k) continue;
    if (index.has(k)) dupes++;
    else index.set(k, r);
  }

  let matched = 0;
  const unmatchedGeo = [], sampleGeo = new Set();
  for (const f of features) {
    const k = normKey(f?.properties?.[geoKey], opts);
    if (k && index.has(k)) matched++;
    else if (sampleGeo.size < 5) {
      const raw = f?.properties?.[geoKey];
      if (!sampleGeo.has(raw)) { sampleGeo.add(raw); unmatchedGeo.push(raw); }
    }
  }

  const usedGeo = new Set(features.map(f => normKey(f?.properties?.[geoKey], opts)));
  const unmatchedCsv = [];
  for (const r of rows) {
    if (unmatchedCsv.length >= 5) break;
    const k = normKey(r[csvKey], opts);
    if (k && !usedGeo.has(k)) unmatchedCsv.push(r[csvKey]);
  }

  return {
    total: features.length, matched,
    rate: features.length ? matched / features.length : 0,
    csvRows: rows.length, csvKeys: index.size, dupes,
    unmatchedGeo, unmatchedCsv, index,
  };
}

/**
 * Applique la jointure et retourne un NOUVEAU GeoJSON — la couche d'origine
 * n'est jamais modifiée, une jointure ratée doit rester sans conséquence.
 */
export function applyJoin(geojson, geoKey, rows, csvKey, columns, opts = {}) {
  const features = geojson?.features || [];
  const { index } = analyzeJoin(features, geoKey, rows, csvKey, opts);
  const prefix = opts.prefix || "";

  const out = features.map(f => {
    const k = normKey(f?.properties?.[geoKey], opts);
    const r = k ? index.get(k) : null;
    const props = { ...(f.properties || {}) };
    for (const c of columns) {
      const name = prefix + c;
      // Les valeurs numériques doivent l'être vraiment, sinon classification
      // graduée et statistiques les traiteraient comme du texte.
      props[name] = r ? coerce(r[c]) : null;
    }
    return { ...f, properties: props };
  });

  return { type: "FeatureCollection", features: out };
}
