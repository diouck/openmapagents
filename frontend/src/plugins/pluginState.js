/**
 * pluginState.js — BRIQUE 1 : état d'installation / activation des plugins.
 * =============================================================================
 * Couche de VISIBILITÉ posée par-dessus l'existant : elle ne touche à aucun
 * module ni au montage des panneaux. Le menu thématique et le modal « Plugins »
 * la consommeront (briques 2 et 3).
 *
 *   installed : ids de modules présents dans le menu (« installés »)
 *   disabled  : installés mais masqués (clic droit → « Désactiver »)
 *   actif     = installed && !disabled
 *
 * Persisté dans localStorage. Premier lancement : tout est installé → le menu
 * est identique à aujourd'hui tant que rien n'est désinstallé.
 * =============================================================================
 */
import { useSyncExternalStore } from "react";
import { MENU_TREE } from "../utils/menuTree";
import { IcTable, IcBarChart, IcStack, IcArrow } from "../icons";

const LS_KEY = "oma.plugins.v1";

/** Catalogue dérivé de MENU_TREE : tous les éléments (outils, indicateurs, à venir). */
const MENU_PLUGINS = MENU_TREE.flatMap((theme) =>
  theme.items
    .filter((it) => ["tool", "indicator", "soon"].includes(it.kind))
    .map((it) => ({
      id: it.id,
      label: it.label,
      icon: it.icon,
      desc: it.desc || "",
      category: theme.label,       // le thème du menu = la catégorie du plugin
      external: false,             // interne (bundlé) ; les tiers viendront en phase 2
      version: "1.0",
      author: "OpenMapAgents",
      tags: [theme.label],
      kind: it.kind,
    }))
);

/** Modules d'analyse accessibles via le menu ⋯ de la légende (hors menu thématique). */
const EXTRA_MODULES = [
  { id: "attr_table",     label: "Table attributaire",        icon: IcTable,    desc: "Table, statistiques, graphiques, filtres, édition de champs" },
  { id: "dashboard",      label: "Tableau de bord",           icon: IcBarChart, desc: "Grille de graphiques multiples d'une couche" },
  { id: "interpolate",    label: "Interpolation (kriging/IDW)", icon: IcStack,  desc: "Couche de points → surface raster continue" },
  { id: "feature_select", label: "Sélection d'entités",       icon: IcArrow,    desc: "Sélection carte par clic / rectangle / polygone" },
].map((m) => ({ ...m, category: "Analyse de données", external: false, version: "1.0", author: "OpenMapAgents", tags: ["Analyse de données"], kind: "module" }));

export const PLUGIN_CATALOG = [...MENU_PLUGINS, ...EXTRA_MODULES];

const CATALOG_INDEX = new Map(PLUGIN_CATALOG.map((p) => [p.id, p]));
export const getPlugin = (id) => CATALOG_INDEX.get(id) || null;

/** Cœur de l'app : toujours présent, non désinstallable. */
export const CORE_IDS = new Set(["layers", "draw", "measure_dist", "measure_area"]);

const ALL_IDS = PLUGIN_CATALOG.map((p) => p.id);

function load() {
  try {
    const raw = JSON.parse(localStorage.getItem(LS_KEY) || "null");
    if (raw && Array.isArray(raw.installed)) {
      return { installed: new Set(raw.installed), disabled: new Set(raw.disabled || []) };
    }
  } catch { /* localStorage indisponible / JSON cassé : on retombe sur le défaut */ }
  return { installed: new Set(ALL_IDS), disabled: new Set() };   // 1er lancement : tout installé
}

let state = load();
const listeners = new Set();

function persist() {
  try {
    localStorage.setItem(LS_KEY, JSON.stringify({
      installed: [...state.installed],
      disabled: [...state.disabled],
    }));
  } catch { /* quota / navigation privée : sans effet, l'état vit en mémoire */ }
}

function commit(next) {
  state = next;
  persist();
  listeners.forEach((l) => l());
}

export function isInstalled(id) { return state.installed.has(id); }
export function isDisabled(id)  { return state.disabled.has(id); }
export function isActive(id)    { return state.installed.has(id) && !state.disabled.has(id); }

/** L'id d'un module actif est-il visible dans le menu ? (utilisé par la brique 2) */
export function isVisible(id)   { return CORE_IDS.has(id) || isActive(id); }

export function install(id) {
  const installed = new Set(state.installed); installed.add(id);
  const disabled = new Set(state.disabled);  disabled.delete(id);
  commit({ installed, disabled });
}

export function uninstall(id) {
  if (CORE_IDS.has(id)) return;                       // cœur : non désinstallable
  const installed = new Set(state.installed); installed.delete(id);
  const disabled = new Set(state.disabled);  disabled.delete(id);
  commit({ installed, disabled });
}

/** Désactiver = garder installé mais masquer du menu (réversible). */
export function setDisabled(id, off) {
  if (CORE_IDS.has(id)) return;
  const disabled = new Set(state.disabled);
  if (off) disabled.add(id); else disabled.delete(id);
  commit({ installed: state.installed, disabled });
}

function subscribe(cb) { listeners.add(cb); return () => listeners.delete(cb); }
function getSnapshot() { return state; }

/** Hook React : re-render quand l'état plugins change (install / disable / uninstall). */
export function usePluginState() { return useSyncExternalStore(subscribe, getSnapshot); }
