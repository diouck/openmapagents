/**
 * menuSearch.js — Moteur de recherche des indicateurs + outils du menu.
 *
 * Construit un index plat à partir de menuTree.js (INDICATORS + MENU_TREE) et
 * fournit une recherche insensible à la casse/accents. Utilisé à la fois par la
 * barre de recherche du ThematicMenu et par la palette Ctrl+K (SearchPalette).
 *
 * Chaque entrée : { kind:"indicator"|"tool", id, label, full, sub, icon, keywords }
 *   - indicator → onIndicator(id) / openModal ; tool → onActivate(id).
 */
import { MENU_TREE, INDICATORS } from "./menuTree";

// minuscule + suppression des accents (é→e) via décomposition NFD.
const _norm = (s) => (s || "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "");

export function buildSearchIndex() {
  const out = [];
  for (const theme of MENU_TREE) {
    for (const it of theme.items) {
      if (it.kind === "tool") {
        out.push({
          kind: "tool", id: it.id, label: it.label, full: it.label,
          sub: theme.label, desc: it.desc, icon: it.icon,
          // La description entre dans l'index : chercher « houppier » ou
          // « balayage » doit ramener l'outil, pas seulement son titre.
          keywords: _norm(`${it.label} ${it.desc || ""} ${theme.label} ${it.id}`),
        });
      } else {
        const ind = INDICATORS[it.id];
        if (!ind) continue;
        const opts = ind.options || [];
        const sats = opts.map(o => o.label).join(" · ");
        const short = ind.title.split(" — ")[0].split(" (")[0];
        out.push({
          kind: "indicator", id: it.id, label: short, full: ind.title,
          sub: sats || theme.label, desc: ind.desc, icon: ind.icon,
          keywords: _norm(`${ind.title} ${ind.desc || ""} ${opts.map(o => `${o.label} ${o.index} ${o.dataset}`).join(" ")} ${theme.label} ${it.id}`),
        });
      }
    }
  }
  return out;
}

export function searchMenu(index, query) {
  const s = _norm(query).trim();
  if (!s) return [];
  const terms = s.split(/\s+/).filter(Boolean);
  const res = [];
  for (const e of index) {
    if (!terms.every(t => e.keywords.includes(t))) continue;   // AND sur tous les mots
    const lab = _norm(e.label), full = _norm(e.full);
    let score = 0;
    if (lab === s) score += 200;
    else if (lab.startsWith(s) || full.startsWith(s)) score += 100;
    else if (lab.includes(s) || full.includes(s)) score += 40;
    for (const t of terms) { if (lab.includes(t)) score += 12; else if (full.includes(t)) score += 6; }
    if (e.kind === "indicator") score += 2;   // léger boost indicateurs
    res.push({ ...e, score });
  }
  return res.sort((a, b) => b.score - a.score || a.label.localeCompare(b.label));
}
