/**
 * pluginContract.js — MAQUETTE (à valider avant tout refactor).
 * =============================================================================
 * Rien ici n'est encore branché : c'est la PROPOSITION de contrat de plugin.
 * Objectif : rendre chaque module (ShadowPanel, GEEPanel, EditorPanel…) déclaratif
 * et chargé à la demande, SANS changer le style ni la forme actuels.
 *
 * On reprend EXACTEMENT la forme des items de `menuTree.js`
 *   { kind, id, label, icon }
 * et on y ajoute juste :
 *   - `load`  : import lazy du panneau (code-splitting Vite, déjà utilisé pour deck.gl)
 *   - `category` : le thème du menu (déjà présent implicitement dans MENU_TREE)
 *   - `roi`   : l'emprise de travail, factorisée (aujourd'hui recodée dans chaque panneau)
 *
 * Migration = zéro réécriture du corps d'un panneau : on l'enveloppe, on lui passe
 * le même `mapRef`/`layers`/`addLayer` qu'avant, plus un `roi` partagé.
 * =============================================================================
 */

/**
 * L'EMPRISE DE TRAVAIL commune à tous les plugins.
 *
 * Aujourd'hui chaque panneau la recalcule (cf. ShadowPanel : « vue courante,
 * emprise d'une couche, ou ROI dessiné »). On la factorise ici une seule fois :
 * un plugin appelle `ctx.roi.getGeoJSON()` et n'a plus à gérer le dessin, la vue,
 * ni les bornes de couche lui-même.
 *
 * @typedef {Object} PluginRoi
 * @property {"view"|"layer"|"draw"} mode
 *   Source choisie par l'utilisateur : la vue courante, l'emprise d'une couche, ou un ROI dessiné.
 * @property {string|null} layerId
 *   Couche de référence quand mode === "layer".
 * @property {() => (object|null)} getGeoJSON
 *   Polygone d'emprise courant (Feature/Polygon GeoJSON) — bbox de la vue, emprise de la
 *   couche, ou ROI dessiné. `null` si aucune emprise valide.
 * @property {() => [number, number, number, number]} getBBox
 *   [minLon, minLat, maxLon, maxLat] de l'emprise courante.
 * @property {(mode: "view"|"layer"|"draw", layerId?: string) => void} setMode
 *   Change la source d'emprise (relie le sélecteur d'emprise du panneau).
 * @property {() => void} startDraw
 *   Démarre le dessin d'un ROI (réutilise l'outil de dessin existant).
 * @property {(cb: (roi: PluginRoi) => void) => (() => void)} onChange
 *   S'abonne aux changements d'emprise ; retourne une fonction de désabonnement.
 */

/**
 * LE CONTEXTE reçu par le composant d'un plugin.
 *
 * Normalise ce que ShadowPanel reçoit DÉJÀ ({ mapRef, layers, basemap, setBasemap })
 * en y ajoutant l'accès carte, l'ajout de couche (= addLayer d'App.jsx) et le ROI.
 *
 * @typedef {Object} PluginContext
 * @property {import("react").RefObject<any>} mapRef   La carte (wrapper MapLibre/Mapbox), comme aujourd'hui.
 * @property {"maplibre"|"mapbox"} engine              Moteur de rendu actif (cf. MAP_ENGINE).
 * @property {Array<object>} layers                    Couches courantes (comme la prop `layers`).
 * @property {(geojson: object, name: string, kind?: string) => void} addLayer
 *   Ajoute une couche à la carte (= `addLayer` d'App.jsx).
 * @property {(id: string) => void} removeLayer        Retire une couche.
 * @property {PluginRoi} roi                           L'emprise de travail partagée (voir PluginRoi).
 * @property {string} apiBase                          Préfixe API (config.API, ex. "/api").
 * @property {object} theme                            Contexte de thème (useThemeContext()).
 * @property {(patch: object) => void} setBasemap      Change le fond de carte (comme setBasemap).
 * @property {() => void} close                        Ferme le panneau (= activateItem("pointer")).
 */

/**
 * UN PLUGIN = une entrée de menu + un panneau (+ un router backend optionnel).
 *
 * Superset de l'item `menuTree.js` actuel { kind, id, label, icon } : mêmes champs,
 * plus `load` (composant lazy) et `category`. Un module existant devient un plugin
 * en 6 lignes, sans toucher à son code.
 *
 * @typedef {Object} Plugin
 * @property {string} id
 *   Identifiant unique ("shadow", "editor", "gee-ndvi"…). Sert aussi au deep-link `?plugin=<id>`.
 * @property {string} label                            Nom affiché dans le menu (comme `label`).
 * @property {import("react").ComponentType<any>} icon Icône Lucide (comme `icon` dans menuTree.js).
 * @property {string} category                         Thème du menu ("Analyse 3D", "Vecteur", "Raster"…).
 * @property {string} [desc]                           Description courte (comme INDICATORS.desc).
 * @property {"panel"|"indicator"|"control"} [kind]    Type d'UI (défaut "panel"). Garde la sémantique de menuTree.
 * @property {() => Promise<{ default: import("react").ComponentType<{ ctx: PluginContext }> }>} load
 *   Import LAZY du panneau (ex. `() => import("../components/ShadowPanel.jsx")`).
 * @property {string} [backendRouter]
 *   Nom du router FastAPI associé (ex. "shadow_routes" → /api/shadow/*). Purement informatif
 *   côté front ; côté back, les routers sont déjà chargés dynamiquement en try/except dans agent.py.
 * @property {number} [minCoreVersion]                 Compat minimale (comme minGeoLibreVersion de GeoLibre).
 * @property {boolean} [external]                      false = plugin interne (bundlé) ; true = tiers (phase 2).
 */

// Maquette : aucun export runtime. Les @typedef servent l'autocomplétion et la doc.
export {};
