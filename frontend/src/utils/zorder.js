/**
 * zorder.js — Ordre d'empilement PARTAGÉ de toutes les fenêtres flottantes.
 *
 * Panneaux (FloatingPanel), fenêtres d'indicateurs (FloatingWindow) et modales
 * flottantes (IndexStatsModal) tirent leur z-index du même compteur : la
 * dernière fenêtre cliquée passe TOUJOURS au-dessus des autres, quel que soit
 * son type. Départ à 3000 : au-dessus des overlays carte (légende, minimap…) et
 * sous les couches bloquantes (palette Ctrl+K 10050, notifications 9999).
 */
let _z = 3000;

/** Prochain z-index (à appeler au montage et à chaque clic de mise au premier plan). */
export function nextZ() {
  return ++_z;
}

/**
 * Remonte `cur` au premier plan, ou le laisse tel quel s'il y est DÉJÀ.
 *
 * À utiliser plutôt que nextZ() sur les clics : la comparaison se fait contre le
 * compteur GLOBAL, pas contre les seules fenêtres du même type. Une fenêtre qui
 * était la plus haute de sa famille mais passée derrière une fenêtre d'un autre
 * type remonte donc bien ; et un clic sur la fenêtre déjà au sommet renvoie la
 * même valeur → React court-circuite le rendu.
 */
export function bumpZ(cur) {
  return cur === _z ? cur : ++_z;
}
