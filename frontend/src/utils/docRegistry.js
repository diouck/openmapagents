/**
 * docRegistry.js — Fabrique le catalogue de la documentation.
 *
 * Une PAGE par indicateur documenté (INDICATOR_DOC) ET par outil (MENU_TREE),
 * assemblée par jointure, sans ressaisie :
 *   • indicateurs : titre/icône/options ← INDICATORS ; abstract/formule/lecture/
 *     limites ← INDICATOR_DOC ; capteur/résolution/… ← SOURCE_META ; usage/exemple
 *     ← DOC_EXTRA.
 *   • outils      : titre/desc/icône/catégorie ← MENU_TREE ; abstract/usage/exemple
 *     ← DOC_TOOLS (sinon repli sur la description du menu).
 *
 * Les URLs sont des SLUGS FRANÇAIS lisibles (SEO), dérivés du titre :
 *   « Occupation du sol (ESA WorldCover) » → occupation-du-sol.
 */
import { INDICATOR_DOC, SOURCE_META, geeCatalog } from "./datasetMeta";
import { INDICATORS, MENU_TREE } from "./menuTree";
import { DOC_CATEGORY_COLOR, DOC_CATEGORY_ORDER, DOC_EXTRA, DOC_SCALES } from "./docContent";
import { DOC_TOOLS } from "./docTools";

// KEY indicateur → { id, label } du thème ; ID outil → idem (1re occurrence).
const CAT_BY_IND = {};
const CAT_BY_TOOL = {};
const TOOL_ITEMS = [];               // outils uniques, dans l'ordre du menu
const seenTool = new Set();
for (const theme of MENU_TREE) {
  for (const item of theme.items) {
    if (item.kind === "indicator") {
      if (!CAT_BY_IND[item.id]) CAT_BY_IND[item.id] = { id: theme.id, label: theme.label };
    } else if (item.kind === "tool") {
      if (item.id.startsWith("planet_")) continue;   // raccourcis planète → ouvrent le viewer, pas de page /doc
      if (!CAT_BY_TOOL[item.id]) {
        CAT_BY_TOOL[item.id] = { id: theme.id, label: theme.label };
        seenTool.add(item.id);
        TOOL_ITEMS.push(item);
      }
    }
  }
}

const stripDiacritics = (s) => s.normalize("NFD").replace(/[̀-ͯ]/g, "");

// Slug SEO à partir du titre français (parenthèses retirées, accents ôtés).
export function seoSlug(title) {
  return stripDiacritics(String(title || ""))
    .replace(/\(.*?\)/g, " ")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

const catColor = (id) => DOC_CATEGORY_COLOR[id] || DOC_CATEGORY_COLOR.autres;

function buildSource(ind) {
  const opts = (ind && ind.options) || [];
  const seen = new Set();
  const sources = [];
  for (const o of opts) {
    if (!o.dataset || seen.has(o.dataset)) continue;
    seen.add(o.dataset);
    const meta = SOURCE_META[o.dataset] || null;
    sources.push({
      dataset: o.dataset,
      label: (meta && meta.label) || o.label || o.dataset,
      provider: meta && meta.provider,
      res: (meta && meta.res) || o.res,
      revisit: meta && meta.revisit,
      coverage: meta && meta.coverage,
      units: meta && meta.units,
      asset: meta && meta.asset,
      catalog: meta && meta.asset ? geeCatalog(meta.asset) : null,
    });
  }
  return sources;
}

// ── Pages « indicateur » ─────────────────────────────────────────────────────
const INDICATOR_PAGES = Object.keys(INDICATOR_DOC).map((key) => {
  const ind = INDICATORS[key] || null;
  const doc = INDICATOR_DOC[key];
  const cat = CAT_BY_IND[key] || { id: "autres", label: "Autres" };
  const extra = DOC_EXTRA[key] || {};
  const sources = buildSource(ind);
  return {
    kind: "indicator",
    key,
    title: (ind && ind.title) || key,
    icon: ind && ind.icon,
    blurb: ind && ind.desc,
    category: cat.id,
    categoryLabel: cat.label,
    color: catColor(cat.id),
    abstract: doc.abstract,
    formula: doc.formula,
    reading: doc.reading,
    caveat: doc.caveat,
    source: sources[0] || null,
    sources,
    dataLine: null,
    usage: extra.usage || null,     // null → repli « satellite » côté article
    example: extra.example || null,
    date: extra.date || null,
    scale: DOC_SCALES[key] || null,
  };
});

// ── Pages « outil » ──────────────────────────────────────────────────────────
const genericToolUsage = (title, catLabel) => [
  `Menu thématique → ${catLabel} → ${title}.`,
  "Réglez les paramètres de l'outil.",
  "Lancez le traitement et lisez le résultat sur la carte.",
];

const TOOL_PAGES = TOOL_ITEMS.map((item) => {
  const cat = CAT_BY_TOOL[item.id] || { id: "outils", label: "Outils & données" };
  const t = DOC_TOOLS[item.id] || {};
  return {
    kind: "tool",
    key: item.id,
    title: item.label || item.id,
    icon: item.icon,
    blurb: item.desc,
    category: cat.id,
    categoryLabel: cat.label,
    color: catColor(cat.id),
    abstract: t.abstract || item.desc || "",
    formula: null,
    reading: null,
    caveat: t.caveat || null,
    source: null,
    sources: [],
    dataLine: t.dataLine || null,
    usage: t.usage || genericToolUsage(item.label || item.id, cat.label),
    example: t.example || null,
    date: t.date || null,
    scale: null,
  };
});

// ── Assemblage, tri, slugs uniques ───────────────────────────────────────────
const catRank = (id) => {
  const i = DOC_CATEGORY_ORDER.indexOf(id);
  return i === -1 ? 999 : i;
};

const PAGES = [...INDICATOR_PAGES, ...TOOL_PAGES];
PAGES.sort((a, b) =>
  catRank(a.category) - catRank(b.category) ||
  a.title.localeCompare(b.title, "fr"));

const _used = new Set();
for (const p of PAGES) {
  let base = seoSlug(p.title) || seoSlug(p.key) || "page";
  let slug = base, i = 2;
  while (_used.has(slug)) slug = `${base}-${i++}`;
  _used.add(slug);
  p.slug = slug;
}

export const DOC_PAGES = PAGES;
export const DOC_BY_SLUG = Object.fromEntries(PAGES.map((p) => [p.slug, p]));

export const DOC_CATEGORIES = (() => {
  const counts = {};
  for (const p of PAGES) counts[p.category] = (counts[p.category] || 0) + 1;
  return Object.keys(counts)
    .sort((a, b) => catRank(a) - catRank(b))
    .map((id) => ({
      id,
      label: (PAGES.find((p) => p.category === id) || {}).categoryLabel || id,
      color: catColor(id),
      count: counts[id],
    }));
})();

export const getDocPage = (slug) => DOC_BY_SLUG[slug] || null;

export function relatedPages(page, limit = 4) {
  if (!page) return [];
  return PAGES.filter((p) => p.category === page.category && p.slug !== page.slug).slice(0, limit);
}

export const RECENT_PAGES = PAGES.filter((p) => p.date).sort((a, b) => b.date.localeCompare(a.date));

export function searchDocs(q) {
  const s = String(q || "").trim().toLowerCase();
  if (!s) return PAGES;
  return PAGES.filter((p) =>
    [p.title, p.abstract, p.blurb, p.key, p.categoryLabel]
      .filter(Boolean)
      .some((t) => t.toLowerCase().includes(s))
  );
}
