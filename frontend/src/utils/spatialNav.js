/**
 * spatialNav.js — Petit store pour piloter la section initiale du hub
 * « Analyse spatiale » (Vecteur / Raster / Avancé) depuis le menu latéral.
 * Le menu appelle setSpatialSection(section) avant d'ouvrir le panneau ; le
 * hub s'abonne pour déplier la bonne famille.
 */
import { useSyncExternalStore } from "react";

let _section = "vecteur";
const listeners = new Set();

export const setSpatialSection = (s) => {
  _section = s;
  listeners.forEach(l => l());
};
export const getSpatialSection = () => _section;

const subscribe = (l) => { listeners.add(l); return () => listeners.delete(l); };
export const useSpatialSection = () => useSyncExternalStore(subscribe, getSpatialSection, getSpatialSection);
