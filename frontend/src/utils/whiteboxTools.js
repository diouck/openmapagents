/**
 * whiteboxTools.js — Catalogue d'outils Whitebox/Analyse raster (métadonnées).
 * =============================================================================
 * Structure : catégories → outils → paramètres.
 *
 * Chaque outil :
 *   - id, name, description (courte), definition (longue, markdown pédago)
 *   - inputs (rasters requis), params (formulaire dynamique), outputs
 *   - implemented : true = calcul backend réel branché (GEE) ; false = préparé,
 *     apparaît dans l'UI mais « Exécuter » est désactivé (badge « Bientôt »).
 *   - engine : "gee" | "whitebox" | "gee|whitebox"
 *
 * On ajoute les catégories/outils AU FUR ET À MESURE : le menu et l'UI les
 * affichent dès qu'ils sont ici, l'implémentation backend suit.
 * =============================================================================
 */

const IN_DEM = [{ id: "dem", label: "MNT (DEM)", type: "raster", required: true, description: "Modèle numérique d'élévation (GeoTIFF)" }];
const IN_RASTER = [{ id: "raster", label: "Raster d'entrée", type: "raster", required: true, description: "Image raster (GeoTIFF)" }];
const P_ZFACTOR = { id: "zfactor", label: "Facteur Z (exagération)", type: "number", default: 1.0, description: "Facteur de verticalité (1.0 = pas d'exagération)" };

export const WHITEBOX_TOOLS = {
  // ══════════════════════════════════════════════════════════════
  // MORPHOLOGIE — implémentée (GEE : ee.Terrain + focal/convolve)
  // ══════════════════════════════════════════════════════════════
  morphologie: {
    name: "Morphologie",
    icon: "mountain",
    description: "Analyse topographique du terrain",
    tools: [
      {
        id: "slope", name: "Slope (Pente)", category: "Morphologie", implemented: true, engine: "gee",
        description: "Angle de pente du terrain",
        definition: `La pente est l'angle ou le gradient du terrain en un point. Elle indique la raideur du relief.

**Interprétation :**
- Pente 0° : terrain plat
- Pente 45° : terrain très escarpé
- 0-5° : plat (agriculture possible)
- 5-15° : modéré (érosion faible)
- >30° : très raide (risque glissement)

**Usages :**
- Agriculture de précision (classement parcelles)
- Aléa glissement (stabilité terrain)
- Infrastructure (routes, bâtiments)
- Hydrologie (ruissellement)`,
        inputs: IN_DEM,
        params: [
          { id: "units", label: "Unité de sortie", type: "select", default: "degrees", options: ["degrees", "percent", "radians"], description: "Degrés, pourcentage ou radians" },
          P_ZFACTOR,
        ],
        outputs: { filename: "slope", format: "GeoTIFF" },
      },
      {
        id: "aspect", name: "Aspect (Orientation)", category: "Morphologie", implemented: true, engine: "gee",
        description: "Direction de pente (exposition)",
        definition: `L'aspect indique la direction de pente (exposition du terrain).

**Valeurs (degrés) :**
- 0° = Nord
- 90° = Est
- 180° = Sud
- 270° = Ouest
- -1 = Plat (pas de pente)

**Usages :**
- Ensoleillement (agriculture, énergie solaire)
- Végétation (versants sud vs nord)
- Érosion (exposition au vent)`,
        inputs: IN_DEM,
        params: [P_ZFACTOR],
        outputs: { filename: "aspect", format: "GeoTIFF" },
      },
      {
        id: "curvature", name: "Curvature (Courbure)", category: "Morphologie", implemented: true, engine: "gee",
        description: "Convexité/concavité du terrain",
        definition: `La courbure décrit la convexité/concavité du terrain.

**Interprétation :**
- Positive = terrain convexe (sommet, crête)
- Négative = terrain concave (vallée)
- Zéro = terrain plat

**Usages :**
- Prédiction érosion (terrain convexe = érosif)
- Hydrologie (localiser cours d'eau)
- Landslide (convergence = accumulation eau)`,
        inputs: IN_DEM,
        params: [
          { id: "type", label: "Type de courbure", type: "select", default: "general", options: ["general", "plan", "profile"], description: "Générale, plan ou profil" },
          P_ZFACTOR,
        ],
        outputs: { filename: "curvature", format: "GeoTIFF" },
      },
      {
        id: "hillshade", name: "Hillshade (Ombrage)", category: "Morphologie", implemented: true, engine: "gee",
        description: "Ombrage du relief pour visualisation",
        definition: `L'ombrage simule un éclairage du MNT pour une visualisation réaliste du relief.

**Paramètres d'éclairage :**
- Azimut : direction du soleil (0=N, 90=E, 180=S, 270=O)
- Altitude : angle du soleil au-dessus de l'horizon (15-90°)

**Notes :**
- Hauteur de soleil 45° = éclairage équilibré
- Azimut 315° (NO) = convention cartographique standard`,
        inputs: IN_DEM,
        params: [
          { id: "azimuth", label: "Azimut du soleil (degrés)", type: "number", default: 315, min: 0, max: 360, description: "Direction de l'éclairage (0=N, 90=E, 180=S, 270=O)" },
          { id: "altitude", label: "Altitude du soleil (degrés)", type: "number", default: 45, min: 0, max: 90, description: "Angle au-dessus de l'horizon" },
          P_ZFACTOR,
        ],
        outputs: { filename: "hillshade", format: "GeoTIFF" },
      },
      {
        id: "tpi", name: "TPI (Position topographique)", category: "Morphologie", implemented: true, engine: "gee",
        description: "Position relative au voisinage",
        definition: `L'indice de position topographique compare l'élévation d'un point avec la moyenne de son voisinage.

**Interprétation :**
- TPI > 0 = point PLUS HAUT que la moyenne = sommet/crête
- TPI < 0 = point PLUS BAS que la moyenne = vallée/dépression
- TPI ≈ 0 = pente régulière

**Usages :**
- Classification du terrain (landforms)
- Habitat/écologie
- Végétation (distribution altitudinale)`,
        inputs: IN_DEM,
        params: [
          { id: "radius", label: "Rayon de voisinage (pixels)", type: "number", default: 10, min: 1, max: 100, description: "Nombre de pixels pour la moyenne locale" },
        ],
        outputs: { filename: "tpi", format: "GeoTIFF" },
      },
      // ── À venir (Whitebox) ──
      { id: "tri", name: "TRI (Rugosité)", category: "Morphologie", implemented: true, engine: "gee", description: "Indice de rugosité du terrain (Riley)", inputs: IN_DEM, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, min: 1, max: 50, description: "Voisinage" }], outputs: { filename: "tri", format: "GeoTIFF" } },
      { id: "roughness", name: "Roughness (Aspérité)", category: "Morphologie", implemented: true, engine: "gee", description: "Écart max d'altitude dans le voisinage", inputs: IN_DEM, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, min: 1, max: 50, description: "Voisinage" }], outputs: { filename: "roughness", format: "GeoTIFF" } },
      { id: "plan_curvature", name: "Courbure planimétrique", category: "Morphologie", implemented: true, engine: "gee", description: "Convergence/divergence horizontale de l'écoulement", inputs: IN_DEM, params: [P_ZFACTOR], outputs: { filename: "plan_curv", format: "GeoTIFF" } },
      { id: "profile_curvature", name: "Courbure de profil", category: "Morphologie", implemented: true, engine: "gee", description: "Accélération/décélération de l'écoulement", inputs: IN_DEM, params: [P_ZFACTOR], outputs: { filename: "prof_curv", format: "GeoTIFF" } },
      { id: "relative_position", name: "Position relative multi-échelle", category: "Morphologie", implemented: true, engine: "gee", description: "TPI normalisé sur plusieurs échelles", inputs: IN_DEM, params: [{ id: "min_radius", label: "Rayon min", type: "number", default: 3, description: "px" }, { id: "max_radius", label: "Rayon max", type: "number", default: 30, description: "px" }], outputs: { filename: "rel_pos", format: "GeoTIFF" } },
    ],
  },

  // ══════════════════════════════════════════════════════════════
  // HYDROLOGIE — préparée (Whitebox réel ou HydroSHEDS, Vague 5)
  // ══════════════════════════════════════════════════════════════
  hydrologie: {
    name: "Hydrologie",
    icon: "droplets",
    description: "Écoulement, bassins et réseaux hydrographiques",
    tools: [
      { id: "fill_depressions", name: "Remplir les dépressions", category: "Hydrologie", implemented: true, engine: "gee", description: "Comble les cuvettes du MNT (pré-requis écoulement)", inputs: IN_DEM, params: [], outputs: { filename: "filled_dem", format: "GeoTIFF" } },
      { id: "flow_direction", name: "Direction d'écoulement (D8)", category: "Hydrologie", implemented: true, engine: "gee", description: "Sens de l'écoulement pixel par pixel", inputs: IN_DEM, params: [], outputs: { filename: "flow_dir", format: "GeoTIFF" } },
      { id: "flow_accumulation", name: "Accumulation d'écoulement", category: "Hydrologie", implemented: true, engine: "gee", description: "Surface drainée cumulée (réseau hydro)", inputs: IN_DEM, params: [], outputs: { filename: "flow_accum", format: "GeoTIFF" } },
      { id: "stream_network", name: "Réseau hydrographique", category: "Hydrologie", implemented: true, engine: "gee", description: "Extraction des cours d'eau par seuil", inputs: IN_DEM, params: [{ id: "threshold", label: "Seuil d'accumulation", type: "number", default: 1000, description: "Pixels drainés min" }], outputs: { filename: "streams", format: "GeoTIFF" } },
      { id: "stream_order", name: "Ordre de Strahler", category: "Hydrologie", implemented: false, engine: "whitebox", description: "Hiérarchie des tronçons de rivière", inputs: IN_DEM, params: [], outputs: { filename: "strahler", format: "GeoTIFF" } },
      { id: "watershed", name: "Bassin versant", category: "Hydrologie", implemented: false, engine: "whitebox", description: "Délimite le bassin depuis un exutoire", inputs: IN_DEM, params: [], outputs: { filename: "watershed", format: "GeoTIFF" } },
    ],
  },

  // ══════════════════════════════════════════════════════════════
  // FILTRES — préparée (GEE convolve, Vague 2)
  // ══════════════════════════════════════════════════════════════
  filtres: {
    name: "Filtres",
    icon: "grid",
    description: "Lissage, netteté et convolutions",
    tools: [
      { id: "mean_filter", name: "Filtre moyen", category: "Filtres", implemented: true, engine: "gee", description: "Lissage par moyenne locale", inputs: IN_RASTER, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, min: 1, max: 25, description: "Taille du noyau" }], outputs: { filename: "mean", format: "GeoTIFF" } },
      { id: "median_filter", name: "Filtre médian", category: "Filtres", implemented: true, engine: "gee", description: "Réduit le bruit en préservant les bords", inputs: IN_RASTER, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, min: 1, max: 25, description: "Taille du noyau" }], outputs: { filename: "median", format: "GeoTIFF" } },
      { id: "gaussian_filter", name: "Filtre gaussien", category: "Filtres", implemented: true, engine: "gee", description: "Lissage pondéré (noyau gaussien)", inputs: IN_RASTER, params: [{ id: "sigma", label: "Sigma", type: "number", default: 1.0, min: 0.5, max: 10, description: "Écart-type du noyau" }], outputs: { filename: "gaussian", format: "GeoTIFF" } },
      { id: "highpass_filter", name: "Filtre passe-haut", category: "Filtres", implemented: true, engine: "gee", description: "Rehausse les détails/contours", inputs: IN_RASTER, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, min: 1, max: 25, description: "Taille du noyau" }], outputs: { filename: "highpass", format: "GeoTIFF" } },
      { id: "sobel_filter", name: "Sobel (contours)", category: "Filtres", implemented: true, engine: "gee", description: "Détection de contours par gradient", inputs: IN_RASTER, params: [], outputs: { filename: "sobel", format: "GeoTIFF" } },
    ],
  },

  // ══════════════════════════════════════════════════════════════
  // NETTOYAGE MNT — préparée (Whitebox réel)
  // ══════════════════════════════════════════════════════════════
  nettoyage: {
    name: "Nettoyage MNT",
    icon: "wrench",
    description: "Correction et conditionnement du MNT",
    tools: [
      { id: "breach_depressions", name: "Percer les dépressions", category: "Nettoyage MNT", implemented: false, engine: "whitebox", description: "Ouvre un exutoire aux cuvettes (moins destructif que remplir)", inputs: IN_DEM, params: [], outputs: { filename: "breached", format: "GeoTIFF" } },
      { id: "fill_missing_data", name: "Combler les trous", category: "Nettoyage MNT", implemented: true, engine: "gee", description: "Interpole les pixels NoData", inputs: IN_DEM, params: [], outputs: { filename: "filled_nodata", format: "GeoTIFF" } },
      { id: "smooth_dem", name: "Lissage du MNT", category: "Nettoyage MNT", implemented: true, engine: "gee", description: "Réduit le bruit d'acquisition", inputs: IN_DEM, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, description: "Voisinage" }], outputs: { filename: "smoothed", format: "GeoTIFF" } },
    ],
  },

  // ══════════════════════════════════════════════════════════════
  // SEGMENTATION — préparée (Whitebox, Vague 2)
  // ══════════════════════════════════════════════════════════════
  segmentation: {
    name: "Segmentation",
    icon: "boxes",
    description: "Regroupement de pixels en objets",
    tools: [
      { id: "connected_components", name: "Composantes connexes", category: "Segmentation", implemented: true, engine: "gee", description: "Étiquette les groupes de pixels contigus", inputs: IN_RASTER, params: [], outputs: { filename: "components", format: "GeoTIFF" } },
      { id: "clump", name: "Clump (agrégats)", category: "Segmentation", implemented: true, engine: "gee", description: "Regroupe les pixels de même valeur", inputs: IN_RASTER, params: [{ id: "diag", label: "Inclure diagonales", type: "select", default: "oui", options: ["oui", "non"], description: "Connexité 8 vs 4" }], outputs: { filename: "clumps", format: "GeoTIFF" } },
      { id: "sieve", name: "Sieve (nettoyage)", category: "Segmentation", implemented: true, engine: "gee", description: "Supprime les petits objets sous un seuil", inputs: IN_RASTER, params: [{ id: "min_size", label: "Taille min (pixels)", type: "number", default: 10, description: "Objets plus petits fusionnés" }], outputs: { filename: "sieved", format: "GeoTIFF" } },
    ],
  },

  // ══════════════════════════════════════════════════════════════
  // STATS LOCALES — préparée (GEE reduceNeighborhood, Vague 2)
  // ══════════════════════════════════════════════════════════════
  stats_locales: {
    name: "Stats locales",
    icon: "chart",
    description: "Statistiques de voisinage (focales)",
    tools: [
      { id: "local_mean", name: "Moyenne locale", category: "Stats locales", implemented: true, engine: "gee", description: "Moyenne dans un voisinage", inputs: IN_RASTER, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, description: "Voisinage" }], outputs: { filename: "local_mean", format: "GeoTIFF" } },
      { id: "local_std", name: "Écart-type local", category: "Stats locales", implemented: true, engine: "gee", description: "Variabilité dans un voisinage", inputs: IN_RASTER, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, description: "Voisinage" }], outputs: { filename: "local_std", format: "GeoTIFF" } },
      { id: "local_max", name: "Maximum local", category: "Stats locales", implemented: true, engine: "gee", description: "Valeur max du voisinage", inputs: IN_RASTER, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, description: "Voisinage" }], outputs: { filename: "local_max", format: "GeoTIFF" } },
      { id: "local_median", name: "Médiane locale", category: "Stats locales", implemented: true, engine: "gee", description: "Médiane du voisinage", inputs: IN_RASTER, params: [{ id: "radius", label: "Rayon (pixels)", type: "number", default: 3, description: "Voisinage" }], outputs: { filename: "local_median", format: "GeoTIFF" } },
    ],
  },

  // ══════════════════════════════════════════════════════════════
  // IMAGE — préparée (GEE, Vague 2)
  // ══════════════════════════════════════════════════════════════
  image: {
    name: "Image / Texture",
    icon: "image",
    description: "Texture, arêtes et rehaussement",
    tools: [
      { id: "edge_detection", name: "Détection d'arêtes", category: "Image", implemented: true, engine: "gee", description: "Contours (Canny/Sobel)", inputs: IN_RASTER, params: [{ id: "threshold", label: "Seuil", type: "number", default: 0.5, description: "Sensibilité" }], outputs: { filename: "edges", format: "GeoTIFF" } },
      { id: "texture", name: "Texture (GLCM)", category: "Image", implemented: true, engine: "gee", description: "Contraste/homogénéité local", inputs: IN_RASTER, params: [{ id: "size", label: "Fenêtre (pixels)", type: "number", default: 3, description: "Taille" }], outputs: { filename: "texture", format: "GeoTIFF" } },
      { id: "directional", name: "Filtre directionnel", category: "Image", implemented: true, engine: "gee", description: "Rehausse les structures selon un angle", inputs: IN_RASTER, params: [{ id: "azimuth", label: "Direction (degrés)", type: "number", default: 45, description: "Orientation" }], outputs: { filename: "directional", format: "GeoTIFF" } },
    ],
  },

  // ══════════════════════════════════════════════════════════════
  // DISTANCE — préparée (GEE cumulativeCost, Vague 2)
  // ══════════════════════════════════════════════════════════════
  distance: {
    name: "Distance",
    icon: "navigation",
    description: "Distances euclidienne et de coût",
    tools: [
      { id: "euclidean_distance", name: "Distance euclidienne", category: "Distance", implemented: true, engine: "gee", description: "Distance à l'objet le plus proche", inputs: IN_RASTER, params: [], outputs: { filename: "eucl_dist", format: "GeoTIFF" } },
      { id: "cost_distance", name: "Distance de coût", category: "Distance", implemented: true, engine: "gee", description: "Coût cumulé de déplacement", inputs: [{ id: "source", label: "Sources", type: "raster", required: true, description: "Points/zones de départ" }, { id: "cost", label: "Surface de coût", type: "raster", required: true, description: "Résistance au déplacement" }], params: [], outputs: { filename: "cost_dist", format: "GeoTIFF" } },
    ],
  },

  // ══════════════════════════════════════════════════════════════
  // RECLASSIFICATION — préparée (GEE remap/where, Vague 2)
  // ══════════════════════════════════════════════════════════════
  reclassification: {
    name: "Reclassification",
    icon: "sliders",
    description: "Seuils, classes et normalisation",
    tools: [
      { id: "threshold", name: "Seuillage binaire", category: "Reclassification", implemented: true, engine: "gee", description: "0/1 selon un seuil", inputs: IN_RASTER, params: [{ id: "value", label: "Seuil", type: "number", default: 0, description: "Valeur de coupure" }], outputs: { filename: "threshold", format: "GeoTIFF" } },
      { id: "slice", name: "Découpage en classes", category: "Reclassification", implemented: true, engine: "gee", description: "Classe les valeurs en tranches", inputs: IN_RASTER, params: [{ id: "n_classes", label: "Nombre de classes", type: "number", default: 5, min: 2, max: 12, description: "Tranches" }], outputs: { filename: "slices", format: "GeoTIFF" } },
      { id: "normalize", name: "Normalisation 0-1", category: "Reclassification", implemented: true, engine: "gee", description: "Étire les valeurs entre 0 et 1", inputs: IN_RASTER, params: [], outputs: { filename: "normalized", format: "GeoTIFF" } },
    ],
  },

  // ══════════════════════════════════════════════════════════════
  // AVANCÉ — préparée (Vague 3)
  // ══════════════════════════════════════════════════════════════
  avance: {
    name: "Avancé",
    icon: "sparkles",
    description: "Réduction, interpolation, classification",
    tools: [
      { id: "pca", name: "ACP (PCA)", category: "Avancé", implemented: true, engine: "gee", description: "Réduction de dimension multibande", inputs: [{ id: "raster", label: "Raster multibande", type: "raster", required: true, description: "Plusieurs bandes" }], params: [{ id: "components", label: "Composantes", type: "number", default: 3, description: "Nombre à garder" }], outputs: { filename: "pca", format: "GeoTIFF" } },
      { id: "kriging", name: "Krigeage", category: "Avancé", implemented: false, engine: "whitebox", description: "Interpolation géostatistique", inputs: [{ id: "points", label: "Points de mesure", type: "vector", required: true, description: "Échantillons" }], params: [], outputs: { filename: "kriging", format: "GeoTIFF" } },
      { id: "kmeans", name: "Classification k-means", category: "Avancé", implemented: true, engine: "gee", description: "Segmentation non supervisée", inputs: [{ id: "raster", label: "Raster multibande", type: "raster", required: true, description: "Plusieurs bandes" }], params: [{ id: "clusters", label: "Nombre de classes", type: "number", default: 5, min: 2, max: 20, description: "Clusters" }], outputs: { filename: "kmeans", format: "GeoTIFF" } },
    ],
  },
};

// Flatten pour accès rapide : WHITEBOX_TOOLS_BY_ID["slope"] → objet complet
export const WHITEBOX_TOOLS_BY_ID = {};
Object.values(WHITEBOX_TOOLS).forEach(cat => {
  cat.tools.forEach(tool => { WHITEBOX_TOOLS_BY_ID[tool.id] = tool; });
});

export const getAllTools = () => {
  const all = [];
  Object.values(WHITEBOX_TOOLS).forEach(cat => all.push(...cat.tools));
  return all;
};

export const getToolsByCategory = (categoryKey) => WHITEBOX_TOOLS[categoryKey]?.tools || [];
export const getCategoryName = (categoryKey) => WHITEBOX_TOOLS[categoryKey]?.name || "";

// Liste des catégories (pour menu + navigation) avec compteurs
export const getCategories = () =>
  Object.entries(WHITEBOX_TOOLS).map(([key, cat]) => ({
    key,
    name: cat.name,
    icon: cat.icon,
    description: cat.description,
    total: cat.tools.length,
    ready: cat.tools.filter(t => t.implemented).length,
  }));
