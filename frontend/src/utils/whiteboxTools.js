/**
 * whiteboxTools.js — Catalogue d'outils Whitebox avec métadonnées
 * =============================================================================
 * Structure : catégories → outils → paramètres
 *
 * Chaque outil a :
 *   - id, name, description (courte), definition (longue pédago)
 *   - inputs (rasters requis)
 *   - params (formulaire dynamique)
 *   - outputs (nom fichier sortie)
 * =============================================================================
 */

export const WHITEBOX_TOOLS = {
  morphologie: {
    name: "Morphologie",
    icon: "ti-mountain",
    description: "Analyse topographique du terrain",
    tools: [
      {
        id: "slope",
        name: "Slope (Pente)",
        category: "Morphologie",
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
        inputs: [
          { id: "dem", label: "MNT (DEM)", type: "raster", required: true, description: "Modèle numérique d'élévation (GeoTIFF)" }
        ],
        params: [
          { id: "units", label: "Unité de sortie", type: "select", default: "degrees", options: ["degrees", "percent", "radians"], description: "Degrés, pourcentage ou radians" },
          { id: "zfactor", label: "Facteur Z (exagération)", type: "number", default: 1.0, description: "Facteur de verticalité (1.0 = pas d'exagération)" }
        ],
        outputs: { filename: "slope", format: "GeoTIFF" }
      },
      {
        id: "aspect",
        name: "Aspect (Orientation)",
        category: "Morphologie",
        definition: `L'aspect indique la direction de pente (exposition du terrain).

        **Valeurs (degrés) :**
        - 0° = Nord
        - 90° = Est
        - 180° = Sud
        - 270° = Ouest
        - -1 = Plat (pas de pente)

        **Usages :**
        - Ensoleillement (agriculture, énergie solaire)
        - Végétation (versants sud vs nord ont flore différente)
        - Érosion (exposition au vent)
        - Géomorphologie (dynamique pentes)`,
        inputs: [
          { id: "dem", label: "MNT (DEM)", type: "raster", required: true, description: "Modèle numérique d'élévation" }
        ],
        params: [
          { id: "zfactor", label: "Facteur Z", type: "number", default: 1.0, description: "Facteur de verticalité" }
        ],
        outputs: { filename: "aspect", format: "GeoTIFF" }
      },
      {
        id: "curvature",
        name: "Curvature (Courbure)",
        category: "Morphologie",
        definition: `La courbure décrit la convexité/concavité du terrain.

        **Interprétation :**
        - Positive = terrain convexe (sommet, crête) = convergent
        - Négative = terrain concave (vallée) = divergent
        - Zéro = terrain plat

        **Plan vs Profile Curvature :**
        - Plan curvature = convergence est-ouest (retient/disperse eau horizontalement)
        - Profile curvature = accélération/décélération pente (érosion/dépôt)

        **Usages :**
        - Prédiction érosion (terrain convexe = érosif)
        - Hydrologie (localiser cours d'eau)
        - Landslide (convergence = accumulation eau → instabilité)`,
        inputs: [
          { id: "dem", label: "MNT (DEM)", type: "raster", required: true, description: "Modèle numérique d'élévation" }
        ],
        params: [
          { id: "type", label: "Type de courbure", type: "select", default: "general", options: ["general", "plan", "profile"], description: "Générale, plan ou profile" },
          { id: "zfactor", label: "Facteur Z", type: "number", default: 1.0, description: "Exagération verticale" }
        ],
        outputs: { filename: "curvature", format: "GeoTIFF" }
      },
      {
        id: "hillshade",
        name: "Hillshade (Ombrage)",
        category: "Morphologie",
        definition: `L'ombrage simule un éclairage du MNT pour une visualisation réaliste du relief.

        **Paramètres d'éclairage :**
        - Azimut : direction du soleil (0°=Nord, 90°=Est, 180°=Sud, 270°=Ouest)
        - Altitude : angle du soleil au-dessus de l'horizon (15-90°)

        **Usages :**
        - Visualisation 3D du terrain (cartes)
        - Interprétation manuelle du relief
        - Fond de carte pour superposer données
        - Qualité esthétique (communication)

        **Notes :**
        - Hauteur de soleil 45° = éclairage équilibré
        - Azimut 315° (NW) = convention cartographique standard`,
        inputs: [
          { id: "dem", label: "MNT (DEM)", type: "raster", required: true, description: "Modèle numérique d'élévation" }
        ],
        params: [
          { id: "azimuth", label: "Azimut du soleil (degrés)", type: "number", default: 315, min: 0, max: 360, description: "Direction de l'éclairage (0=N, 90=E, 180=S, 270=W)" },
          { id: "altitude", label: "Altitude du soleil (degrés)", type: "number", default: 45, min: 0, max: 90, description: "Angle au-dessus de l'horizon" },
          { id: "zfactor", label: "Facteur Z", type: "number", default: 1.0, description: "Exagération de relief" }
        ],
        outputs: { filename: "hillshade", format: "GeoTIFF" }
      },
      {
        id: "tpi",
        name: "TPI (Topographic Position Index)",
        category: "Morphologie",
        definition: `L'indice de position topographique compare l'élévation d'un point avec la moyenne de son voisinage.

        **Interprétation :**
        - TPI > 0 = point PLUS HAUT que la moyenne = sommet/crête
        - TPI < 0 = point PLUS BAS que la moyenne = vallée/dépression
        - TPI ≈ 0 = point proche de la moyenne = pente régulière

        **Classes typiques (Weiss, 2001) :**
        - >1 std : pics/crêtes
        - 0 à 1 std : hauts reliefs
        - -0.5 à 0.5 std : pentes régulières
        - -1 à -0.5 std : bas reliefs
        - <-1 std : vallées/ravins

        **Usages :**
        - Classification du terrain (landforms)
        - Habitat/écologie (espèces préfèrent sommets vs vallées)
        - Érosion (crêtes = plus érodées)
        - Végétation (distribution altitudinale)`,
        inputs: [
          { id: "dem", label: "MNT (DEM)", type: "raster", required: true, description: "Modèle numérique d'élévation" }
        ],
        params: [
          { id: "radius", label: "Rayon de voisinage (pixels)", type: "number", default: 10, min: 1, max: 100, description: "Nombre de pixels pour calculer la moyenne locale" }
        ],
        outputs: { filename: "tpi", format: "GeoTIFF" }
      }
    ]
  }
};

// Flatten pour accès rapide : WHITEBOX_TOOLS_BY_ID["slope"] → objet complet
export const WHITEBOX_TOOLS_BY_ID = {};
Object.values(WHITEBOX_TOOLS).forEach(cat => {
  cat.tools.forEach(tool => {
    WHITEBOX_TOOLS_BY_ID[tool.id] = tool;
  });
});

// Liste plates pour menus
export const getAllTools = () => {
  const all = [];
  Object.values(WHITEBOX_TOOLS).forEach(cat => {
    all.push(...cat.tools);
  });
  return all;
};

export const getToolsByCategory = (categoryKey) => {
  return WHITEBOX_TOOLS[categoryKey]?.tools || [];
};

export const getCategoryName = (categoryKey) => {
  return WHITEBOX_TOOLS[categoryKey]?.name || "";
};
