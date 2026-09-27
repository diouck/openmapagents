/**
 * spatialNav.js — Petit store pour piloter la section initiale du hub
 * « Analyse spatiale » (Vecteur / Raster / Avancé) depuis le menu latéral.
 * Le menu appelle setSpatialSection(section) avant d'ouvrir le panneau ; le
 * hub s'abonne pour déplier la bonne famille.
 */
import { useSyncExternalStore } from "react";

// _nav : { section, category, tool, nonce } — nonce force une notif même si identique
let _nav = { section: "vecteur", category: null, tool: null, nonce: 0 };
const listeners = new Set();

// Pilote la navigation depuis le menu latéral : famille + catégorie + outil (optionnels)
export const setSpatialTarget = (section, category = null, tool = null) => {
  _nav = { section, category, tool, nonce: _nav.nonce + 1 };
  listeners.forEach(l => l());
};
// Compat : ancienne API section seule
export const setSpatialSection = (s) => setSpatialTarget(s, null);
export const getSpatialNav = () => _nav;

const subscribe = (l) => { listeners.add(l); return () => listeners.delete(l); };
export const useSpatialNav = () => useSyncExternalStore(subscribe, getSpatialNav, getSpatialNav);
