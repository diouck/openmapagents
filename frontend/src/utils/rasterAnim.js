/**
 * rasterAnim.js — Échange de tuiles raster SANS clignotement (ping-pong A/B).
 *
 * MapLibre ne sait pas remplacer les `tiles` d'une source existante : il faut
 * supprimer puis recréer la source, ce qui produit un flash blanc à chaque pas —
 * inacceptable pour une animation.
 *
 * On maintient donc DEUX couches raster par couche animée. La frame suivante est
 * chargée dans la couche masquée ; on n'échange les opacités qu'une fois ses
 * tuiles réellement arrivées. L'image à l'écran n'est jamais retirée avant que
 * la suivante ne soit prête.
 */

const _st = new Map();   // baseId → { slot: 0|1, pending: {url, promise}|null }

const slotIds = (baseId, s) => ({ src: `${baseId}__tl${s}`, lyr: `${baseId}__tl${s}-layer` });

/** Couche vectorielle la plus basse : on glisse l'animation dessous pour ne pas
 *  masquer les contours/étiquettes de l'utilisateur. */
function beforeVectorLayer(map) {
  try {
    for (const l of map.getStyle()?.layers || []) {
      if (/-(fill|line|circle|label|3dlabel|extrude)$/.test(l.id)) return l.id;
    }
  } catch (_) { /* style pas prêt */ }
  return undefined;
}

function removeSlot(map, baseId, s) {
  const { src, lyr } = slotIds(baseId, s);
  try { if (map.getLayer(lyr))  map.removeLayer(lyr); } catch (_) {}
  try { if (map.getSource(src)) map.removeSource(src); } catch (_) {}
}

/** Attend que la source ait fini de charger ses tuiles (avec garde-fou). */
function waitSource(map, srcId, timeout = 2500) {
  return new Promise(resolve => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      map.off("sourcedata", onData);
      clearTimeout(t);
      resolve();
    };
    const onData = (e) => {
      if (e.sourceId === srcId && e.isSourceLoaded) finish();
    };
    // Réseau lent / tuile absente : on bascule quand même, sinon l'animation se fige.
    const t = setTimeout(finish, timeout);
    try {
      if (map.getSource(srcId) && map.isSourceLoaded(srcId)) return finish();
    } catch (_) {}
    map.on("sourcedata", onData);
  });
}

/**
 * Charge `tileUrl` dans le slot masqué SANS l'afficher.
 *
 * Séparer le chargement de l'affichage est ce qui rend l'animation fluide : on
 * prépare l'image suivante pendant que la courante est à l'écran, si bien que
 * la bascule ne coûte plus rien. Auparavant chaque pas payait « chargement PUIS
 * délai », d'où une cadence réelle très inférieure à celle demandée.
 *
 * Résout quand les tuiles sont arrivées (ou au bout du garde-fou).
 */
export function preloadFrame(map, baseId, tileUrl, fade = 0) {
  if (!map || !tileUrl) return Promise.resolve();
  const st = _st.get(baseId) || { slot: 1, pending: null };
  _st.set(baseId, st);
  if (st.pending && st.pending.url === tileUrl) return st.pending.promise;  // déjà en vol

  const next = st.slot === 0 ? 1 : 0;
  const { src, lyr } = slotIds(baseId, next);
  removeSlot(map, baseId, next);
  // Le fondu enchaîné est porté par la couche elle-même (transition d'opacité posée
  // à la création) : la bascule d'opacité au commit s'anime alors toute seule, sans
  // toucher à une propriété null au vol.
  const paint = { "raster-opacity": 0, "raster-fade-duration": 0 };
  if (fade > 0) paint["raster-opacity-transition"] = { duration: fade, delay: 0 };
  try {
    map.addSource(src, { type: "raster", tiles: [tileUrl], tileSize: 256 });
    map.addLayer({ id: lyr, type: "raster", source: src, paint }, beforeVectorLayer(map));
  } catch (e) {
    console.warn("[rasterAnim] addSource:", e);
    return Promise.resolve();
  }
  const promise = waitSource(map, src);
  st.pending = { url: tileUrl, promise };
  return promise;
}

/**
 * Bascule sur la frame préchargée (aucune attente réseau). Si la couche a été
 * créée avec un `fade` > 0, le changement d'opacité s'anime en FONDU ENCHAÎNÉ
 * (la nouvelle monte pendant que l'ancienne descend) → plus de clignotement.
 */
export function commitFrame(map, baseId, opacity = 0.85) {
  const st = _st.get(baseId);
  if (!map || !st || !st.pending) return;
  const next = st.slot === 0 ? 1 : 0;
  const { lyr } = slotIds(baseId, next);
  try {
    if (!map.getLayer(lyr)) return;
    map.setPaintProperty(lyr, "raster-opacity", opacity);
    const prev = slotIds(baseId, st.slot);
    if (map.getLayer(prev.lyr)) map.setPaintProperty(prev.lyr, "raster-opacity", 0);
  } catch (_) { return; }
  st.slot = next;
  st.pending = null;
}

/** Charge puis affiche (confort : préchargement + bascule en un appel). */
export async function showFrame(map, baseId, tileUrl, opacity = 0.85, fade = 0) {
  await preloadFrame(map, baseId, tileUrl, fade);
  commitFrame(map, baseId, opacity);
}

/** Met à jour l'opacité de la frame visible (curseur d'opacité du gestionnaire). */
export function setFrameOpacity(map, baseId, opacity) {
  const st = _st.get(baseId); if (!map || !st) return;
  const { lyr } = slotIds(baseId, st.slot);
  try { if (map.getLayer(lyr)) map.setPaintProperty(lyr, "raster-opacity", opacity); } catch (_) {}
}

/** Retire les deux couches d'animation et oublie l'état. */
export function clearAnim(map, baseId) {
  if (map) { removeSlot(map, baseId, 0); removeSlot(map, baseId, 1); }
  _st.delete(baseId);
}
