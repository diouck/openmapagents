/**
 * docContent.js — Contenu ÉDITORIAL de la documentation (le « blog »).
 *
 * Deux choses que la donnée brute (datasetMeta.js) ne porte pas :
 *   • DOC_CATEGORY_COLOR / _ORDER : l'habillage des thèmes dans la doc.
 *   • DOC_EXTRA : par indicateur, deux sections RÉDIGÉES à la main —
 *       - usage   : comment l'obtenir dans l'application (étapes concrètes) ;
 *       - example : un exemple complet, façon étude de cas ;
 *       - date    : date de publication (alimente « Récemment publiés »).
 *
 * Le registre (docRegistry.js) fabrique une page pour CHACUN des 66 indicateurs
 * documentés, qu'il ait ou non une entrée ici : sans DOC_EXTRA, la page se rend
 * à partir du contenu existant (à quoi ça sert / formule / lecture / limites).
 * On enrichit DOC_EXTRA progressivement, sans jamais casser une page.
 */

// Ordre d'affichage des catégories (thèmes MENU_TREE portant indicateurs/outils).
export const DOC_CATEGORY_ORDER = [
  "imagerie", "vegetation", "eau", "ocean", "climat", "meteo", "air", "urbain", "risques", "relief", "outils",
];

// Couleur de repère par catégorie — teintes fixes lisibles en clair ET sombre
// (dérivées de la charte : config.js LAYER_COLORS / theme.js).
export const DOC_CATEGORY_COLOR = {
  imagerie:   "#8a8880",
  vegetation: "#1D9E75",
  eau:        "#378ADD",
  ocean:      "#2CA6BC",
  climat:     "#EF9F27",
  meteo:      "#5AA9E6",
  air:        "#7F77DD",
  urbain:     "#D4537E",
  risques:    "#E24B4A",
  relief:     "#D85A30",
  outils:     "#6E8BA8",
  autres:     "#8a8880",
};

// ── Barres d'échelle de lecture (« Comment le lire ») ────────────────────────
// Par indicateur : un dégradé CSS prêt à l'emploi + graduations + légende.
// Absente → la page affiche seulement le texte de lecture (repli).
const VEG_SCALE = {
  gradient: "linear-gradient(90deg,#2b5f8a 0%,#2b5f8a 12%,#8B4513 12%,#c99a5b 30%,#e6d8a8 44%,#a9c46b 62%,#4a9e3f 80%,#1f6b2a 100%)",
  ticks: ["−1", "0", "0,2", "0,4", "0,6", "+1"],
  legend: [
    { c: "#2b5f8a", t: "Eau, neige < 0" },
    { c: "#8B4513", t: "Sol nu 0,1–0,2" },
    { c: "#e6d8a8", t: "Clairsemé 0,2–0,4" },
    { c: "#4a9e3f", t: "Actif 0,4–0,7" },
    { c: "#1f6b2a", t: "Dense > 0,7" },
  ],
};
const WATER_SCALE = {
  gradient: "linear-gradient(90deg,#8B4513 0%,#DEB887 35%,#f2f2f2 50%,#AED6F1 65%,#1A5276 100%)",
  ticks: ["−0,5", "0", "+0,5"],
  legend: [
    { c: "#8B4513", t: "Terre / bâti < 0" },
    { c: "#AED6F1", t: "Eau > 0" },
  ],
};
export const DOC_SCALES = {
  NDVI: VEG_SCALE, EVI: VEG_SCALE, SAVI: VEG_SCALE, GNDVI: VEG_SCALE,
  NDWI: WATER_SCALE, MNDWI: WATER_SCALE,
  NDMI: {
    gradient: "linear-gradient(90deg,#a0522d 0%,#deb887 35%,#eef2e6 50%,#7fc9c0 70%,#0e7c8c 100%)",
    ticks: ["−1", "0", "+1"],
    legend: [{ c: "#a0522d", t: "Sec / stress" }, { c: "#0e7c8c", t: "Humide" }],
  },
  NBR: {
    gradient: "linear-gradient(90deg,#3a0a0a 0%,#7a2b1a 25%,#c99a5b 50%,#a9c46b 72%,#1f6b2a 100%)",
    ticks: ["−1", "0", "+1"],
    legend: [{ c: "#3a0a0a", t: "Brûlé / faible" }, { c: "#1f6b2a", t: "Végétation saine" }],
  },
  NDSI: {
    gradient: "linear-gradient(90deg,#2b5f8a 0%,#6b8fa8 35%,#cfd8dc 55%,#eef3f6 78%,#ffffff 100%)",
    ticks: ["−1", "0", "0,4", "+1"],
    legend: [{ c: "#2b5f8a", t: "Sans neige" }, { c: "#ffffff", t: "Neige > 0,4" }],
  },
  NDBI: {
    gradient: "linear-gradient(90deg,#1f6b2a 0%,#a9c46b 40%,#e6d8a8 55%,#d8a0a0 75%,#8a3324 100%)",
    ticks: ["−1", "0", "+1"],
    legend: [{ c: "#1f6b2a", t: "Végétation < 0" }, { c: "#8a3324", t: "Bâti > 0" }],
  },
  BSI: {
    gradient: "linear-gradient(90deg,#1f6b2a 0%,#a9c46b 40%,#e6d8a8 60%,#c99a5b 80%,#8B4513 100%)",
    ticks: ["−1", "0", "+1"],
    legend: [{ c: "#1f6b2a", t: "Couvert végétal" }, { c: "#8B4513", t: "Sol nu" }],
  },
};

// ── Contenu rédigé (usage + exemple) ─────────────────────────────────────────
// `usage`   : liste d'étapes (chaînes simples).
// `example` : { title, body, stats?: [{ v, k }] }.
// `date`    : ISO — publication (pour le tri « récents »).
export const DOC_EXTRA = {
  NDVI: {
    date: "2026-07-10",
    usage: [
      "Ouvrez le menu thématique → Végétation & agriculture → NDVI.",
      "Choisissez la source (Sentinel-2 à 10 m, ou Landsat à 30 m pour les longues séries) et la période.",
      "La couche NDVI s'affiche : ajustez la palette et les seuils dans le panneau de style (quantiles ou classes égales).",
      "Ouvrez Statistiques pour lire min / moyenne / max sur une emprise, ou tracez une série temporelle pour suivre l'évolution.",
    ],
    example: {
      title: "Repérer le stress hydrique d'une plaine agricole en été",
      body: "Sur une plaine céréalière en juillet, on calcule le NDVI en Sentinel-2 puis on le compare à une image de juin. Les parcelles qui chutent nettement entre les deux dates trahissent un manque d'eau avant que la couleur visible ne l'annonce. On superpose les limites parcellaires (catalogue vectoriel) pour cibler l'irrigation.",
      stats: [
        { v: "0,71", k: "NDVI juin (sain)" },
        { v: "0,38", k: "NDVI juillet (stress)" },
        { v: "−0,33", k: "Écart · alerte" },
      ],
    },
  },
  MNDWI: {
    date: "2026-07-08",
    usage: [
      "Menu thématique → Eau & humidité → MNDWI (eau urbain).",
      "Source Sentinel-2 (10 m) de préférence en ville : le SWIR y distingue mieux l'eau du bâti que le NDWI.",
      "Seuillez autour de 0 pour isoler l'eau libre, puis vectorisez si besoin (Statistiques / classification).",
    ],
    example: {
      title: "Cartographier l'étendue d'un plan d'eau en zone bâtie",
      body: "Autour d'un lac urbain, le NDWI classe à tort certains toits en eau. Le MNDWI, qui remplace le proche infrarouge par le SWIR, rend le bâti nettement négatif et ne garde que l'eau réelle — un simple seuil à 0 suffit alors à en tracer le contour.",
    },
  },
  NDWI: {
    date: "2026-07-06",
    usage: [
      "Menu thématique → Eau & humidité → NDWI.",
      "Bon marqueur d'eau libre en milieu naturel ; en ville, préférez le MNDWI.",
      "Comparez deux dates pour suivre une mise en eau (crue, remplissage de retenue).",
    ],
    example: {
      title: "Suivre le remplissage d'une retenue au fil des saisons",
      body: "En traçant le NDWI mois par mois sur un barrage, la surface positive croît puis décroît avec la saison des pluies. La série temporelle donne directement la dynamique du plan d'eau.",
    },
  },
  LST: {
    date: "2026-07-05",
    usage: [
      "Menu thématique → Climat & température → Température de surface (LST).",
      "Landsat (30 m) pour le détail urbain, MODIS (1 km) pour un suivi quotidien à large échelle.",
      "La donnée source est en kelvin ; l'affichage la convertit en °C.",
    ],
    example: {
      title: "Mesurer l'îlot de chaleur urbain d'une ville",
      body: "Sur une image d'après-midi d'été, le centre bâti dépasse de plusieurs degrés les campagnes et les parcs voisins. En comparant la LST au NDVI, on relie directement les zones les plus chaudes au déficit de végétation.",
      stats: [
        { v: "+7 °C", k: "Centre vs campagne" },
        { v: "−4 °C", k: "Effet d'un grand parc" },
      ],
    },
  },
  NBR: {
    date: "2026-07-04",
    usage: [
      "Menu thématique → Risques & changements → Ratio de brûlage (NBR).",
      "Calculez le NBR avant et après un feu ; la différence (dNBR) donne la sévérité.",
      "Pour un flux guidé avant/après, utilisez plutôt l'outil « Sévérité d'incendie (dNBR) ».",
    ],
    example: {
      title: "Évaluer les dégâts d'un incendie de forêt",
      body: "Après un feu, le NBR chute là où la végétation a brûlé. La différence avec l'image d'avant sépare les zones intactes, modérément touchées et sévèrement brûlées, selon les seuils UN-SPIDER.",
    },
  },
  NO2: {
    date: "2026-07-03",
    usage: [
      "Menu thématique → Qualité de l'air → Dioxyde d'azote (NO₂).",
      "Source Sentinel-5P (~7 km) ; agrégez sur plusieurs jours pour lisser le bruit et les nuages.",
      "Comparez semaine ouvrée / week-end pour isoler la part du trafic.",
    ],
    example: {
      title: "Visualiser le panache de pollution d'une agglomération",
      body: "En moyennant le NO₂ sur un mois, l'agglomération et ses axes routiers ressortent nettement au-dessus du fond régional — signature du trafic et de l'industrie.",
    },
  },
  SST: {
    date: "2026-07-02",
    usage: [
      "Menu thématique → Océans & littoral → Température de surface de la mer.",
      "MODIS-Aqua (~4 km) pour le détail côtier, NOAA OISST (~27 km) pour un suivi quotidien homogène.",
      "Pour détecter une canicule marine, passez à l'indicateur « Anomalie de SST ».",
    ],
    example: {
      title: "Suivre une vague de chaleur marine",
      body: "Le long d'un littoral, la SST estivale se compare à la normale saisonnière. Une anomalie positive persistante signale une canicule marine, à risque pour les écosystèmes (herbiers, coraux).",
    },
  },
  ELEV: {
    date: "2026-07-01",
    usage: [
      "Menu thématique → Relief & 3D / LiDAR → Élévation.",
      "Copernicus DEM ou SRTM (30 m) ; l'exagération règle l'intensité du relief ombré.",
      "Active le relief 3D dans l'en-tête pour une lecture en perspective.",
    ],
    example: {
      title: "Préparer une analyse de bassin versant",
      body: "L'élévation est le socle de l'hydrologie : à partir d'elle se déduisent pentes, sens d'écoulement et limites de bassin. On la visualise avant de lancer l'outil « Bassin versant ».",
    },
  },

  // ── Imagerie satellite ──
  RGBIMG: {
    usage: [
      "Menu thématique → Imagerie satellite → Image vraies couleurs (RVB).",
      "Choisissez la source (Sentinel-2, Landsat ou MODIS) et la période.",
      "Le masque nuages est désactivé par défaut : élargissez la période pour une mosaïque plus propre.",
    ],
    example: {
      title: "Constater l'état d'un site avant d'interpréter",
      body: "Avant de lire un indice calculé, l'image vraies couleurs vérifie ce qui est réellement au sol — nuages, inondation, chantier, panache — et évite les faux diagnostics.",
    },
  },
  IRCIMG: {
    usage: [
      "Menu thématique → Imagerie satellite → Infrarouge couleur (IRC).",
      "Source Sentinel-2 de préférence, puis la période.",
      "Lisez la végétation en rouge vif, l'eau en bleu-noir, l'urbain en cyan.",
    ],
    example: {
      title: "Juger d'un coup d'œil la vigueur d'un couvert",
      body: "Le proche infrarouge affiché en rouge fait ressortir la végétation dense et active en rouge saturé, et trace nettement la limite eau / terre.",
    },
  },
  SWIRIMG: {
    usage: [
      "Menu thématique → Imagerie satellite → Composite SWIR.",
      "Source Sentinel-2, période autour de l'événement de feu.",
      "Le moyen infrarouge traverse la fumée : front actif en orange, brûlis en brun-rouge.",
    ],
    example: {
      title: "Suivre un feu de forêt à travers la fumée",
      body: "Là où le visible ne montre qu'un panache opaque, le composite SWIR distingue simultanément le front de flamme, la surface déjà brûlée et la végétation intacte.",
    },
  },

  // ── Végétation & agriculture ──
  EVI: {
    usage: [
      "Menu thématique → Végétation & agriculture → EVI.",
      "Source Sentinel-2 et période.",
      "À privilégier là où le NDVI plafonne (couverts très denses).",
    ],
    example: {
      title: "Discriminer des couverts en forêt tropicale",
      body: "Sous forêt dense, le NDVI sature au-dessus de 0,8 ; l'EVI, corrigé des aérosols, continue de séparer les niveaux de biomasse.",
    },
  },
  SAVI: {
    usage: [
      "Menu thématique → Végétation & agriculture → SAVI.",
      "Source Sentinel-2 et période.",
      "Adapté aux milieux peu couverts (le facteur L atténue l'effet du sol).",
    ],
    example: {
      title: "Suivre une culture en début de cycle",
      body: "Quand la végétation ne couvre pas encore le sol, le NDVI est perturbé par le substrat ; le SAVI en corrige l'influence et donne une lecture plus juste.",
    },
  },
  GNDVI: {
    usage: [
      "Menu thématique → Végétation & agriculture → GNDVI.",
      "Source Sentinel-2 ou Landsat et période.",
      "Plus lié à la chlorophylle qu'à la simple biomasse.",
    ],
    example: {
      title: "Piloter la fertilisation azotée",
      body: "Le GNDVI, sensible à l'état nutritionnel du couvert, aide à repérer dans la parcelle les zones à renforcer en azote.",
    },
  },
  NDRE: {
    usage: [
      "Menu thématique → Végétation & agriculture → NDRE.",
      "Source Sentinel-2 uniquement (bande red-edge, absente de Landsat).",
      "Lisez l'évolution relative dans la parcelle plus que la valeur absolue.",
    ],
    example: {
      title: "Détecter un stress avant qu'il ne soit visible",
      body: "La bande red-edge réagit tôt aux variations de chlorophylle : le NDRE signale un stress que le NDVI ne montrera que plus tard.",
    },
  },
  CANOPY: {
    usage: [
      "Menu thématique → Végétation & agriculture → Hauteur de canopée.",
      "Donnée statique (Meta / Global Forest Watch, ~1 m) : pas de période à choisir.",
      "Les valeurs sous 1 m sont masquées.",
    ],
    example: {
      title: "Repérer les arbres hors forêt et les haies",
      body: "À 1 m de résolution, le modèle distingue les alignements et haies bocagères, utiles pour la trame verte et les estimations de biomasse.",
    },
  },
  FOREST: {
    date: "2026-06-28",
    usage: [
      "Menu thématique → Végétation & agriculture → Forêt (Global Forest Watch).",
      "Choisissez la couche : couverture 2000, perte, ou gain.",
      "Donnée statique annuelle (Hansen / GLAD).",
    ],
    example: {
      title: "Établir un bilan de déforestation",
      body: "La couche de perte cumule toutes les disparitions de couvert depuis 2000 — mais coupe, incendie et récolte de plantation y sont mêlés : à ne pas confondre avec de la déforestation nette.",
    },
  },
  BIOMASS: {
    usage: [
      "Menu thématique → Végétation & agriculture → Biomasse aérienne.",
      "Donnée GEDI L4B (1 km), statique.",
      "Valeurs en Mg/ha ; le carbone représente environ la moitié de la masse.",
    ],
    example: {
      title: "Estimer le carbone stocké d'un massif",
      body: "La biomasse GEDI alimente les inventaires carbone et les projets de crédits forestiers — en gardant à l'esprit l'absence de données au-delà de 52° de latitude.",
    },
  },
  GPP: {
    usage: [
      "Menu thématique → Végétation & agriculture → Productivité végétale (GPP).",
      "Source MODIS (composite 8 jours) et période.",
      "Valeurs en kg C/m²/8 j.",
    ],
    example: {
      title: "Comparer la productivité entre saisons",
      body: "La GPP quantifie la photosynthèse brute : on suit sa montée au printemps et son effondrement en période de sécheresse.",
    },
  },
  LAIFAPAR: {
    usage: [
      "Menu thématique → Végétation & agriculture → LAI / FAPAR.",
      "Source MODIS (composite 4 jours) ; choisissez LAI ou FAPAR.",
      "LAI en m²/m² ; FAPAR sans unité (0–1).",
    ],
    example: {
      title: "Suivre la densité foliaire au fil du cycle",
      body: "Le LAI (surface de feuilles par surface de sol) trace le développement du couvert, entrée classique des modèles de rendement.",
    },
  },
  GEDI_CANOPY: {
    date: "2026-06-25",
    usage: [
      "Menu thématique → Végétation & agriculture → GEDI — Hauteur de canopée.",
      "Source GEDI L2A (25 m, composite mensuel) et période.",
      "Couverture entre ~51,6° N et S seulement (orbite de l'ISS).",
    ],
    example: {
      title: "Mesurer la hauteur des arbres par LiDAR spatial",
      body: "GEDI envoie une impulsion laser et analyse l'écho : la hauteur RH98 donne la structure verticale de la forêt, là où les indices optiques ne voient que la densité.",
    },
  },
  GEDI_ELEV: {
    usage: [
      "Menu thématique → Végétation & agriculture → GEDI — Élévation du terrain.",
      "Source GEDI L2A (25 m) et période.",
    ],
    example: {
      title: "Restituer le sol sous un couvert dense",
      body: "Le dernier écho de l'onde LiDAR provient du sol : GEDI donne une altitude du terrain même en forêt, là où les MNT optiques captent le sommet de la canopée.",
    },
  },
  GEDI_COVER: {
    usage: [
      "Menu thématique → Végétation & agriculture → GEDI — Couverture de canopée.",
      "Source GEDI L2B (25 m) et période.",
    ],
    example: {
      title: "Quantifier la fraction de couvert arboré",
      body: "La couverture (%) sépare forêt fermée et milieux ouverts, utile pour caractériser une trouée ou une lisière.",
    },
  },
  GEDI_PAI: {
    usage: [
      "Menu thématique → Végétation & agriculture → GEDI — Indice de surface foliaire (PAI).",
      "Source GEDI L2B (25 m) et période.",
    ],
    example: {
      title: "Décrire la structure d'un couvert",
      body: "Le PAI mesure la densité d'éléments végétaux traversés par le laser : deux forêts de même hauteur mais de structures différentes s'y distinguent.",
    },
  },
  GEDI_AGB: {
    usage: [
      "Menu thématique → Végétation & agriculture → GEDI — Biomasse aérienne (25 m).",
      "Source GEDI L4A (25 m) et période.",
      "Valeurs en Mg/ha à l'empreinte.",
    ],
    example: {
      title: "Estimer le carbone à l'échelle de l'empreinte",
      body: "La biomasse L4A affine les estimations à 25 m, plus fines que la maille kilométrique du L4B pour un suivi local.",
    },
  },
  SOC: {
    usage: [
      "Menu thématique → Végétation & agriculture → Carbone organique du sol.",
      "Donnée OpenLandMap (250 m), statique.",
    ],
    example: {
      title: "Cartographier la fertilité d'un terroir",
      body: "Le stock de carbone organique renseigne la fertilité et la capacité de rétention en eau des sols, utile en diagnostic agronomique.",
    },
  },
  SOILPH: {
    usage: [
      "Menu thématique → Végétation & agriculture → pH du sol.",
      "Donnée OpenLandMap (250 m), statique.",
    ],
    example: {
      title: "Adapter les cultures à l'acidité du sol",
      body: "La carte de pH oriente le choix des espèces et les besoins en amendement (chaulage sur sols acides).",
    },
  },
  CLAY: {
    usage: [
      "Menu thématique → Végétation & agriculture → Teneur en argile.",
      "Donnée OpenLandMap (250 m), statique.",
    ],
    example: {
      title: "Anticiper drainage et irrigation",
      body: "La texture argileuse conditionne la rétention d'eau et le ressuyage : une forte teneur en argile appelle une gestion prudente de l'irrigation.",
    },
  },

  // ── Eau & humidité ──
  NDMI: {
    usage: [
      "Menu thématique → Eau & humidité → NDMI (humidité végétation).",
      "Source Sentinel-2 et période.",
    ],
    example: {
      title: "Suivre le stress hydrique d'une forêt",
      body: "Le NDMI, sensible à la teneur en eau des feuilles, baisse avant le NDVI en début de sécheresse — un signal d'alerte précoce.",
    },
  },
  NDCI: {
    usage: [
      "Menu thématique → Eau & humidité → NDCI (qualité de l'eau).",
      "Source Sentinel-2 et période.",
    ],
    example: {
      title: "Détecter une efflorescence algale",
      body: "Le NDCI approche la concentration en chlorophylle des plans d'eau : une hausse localisée trahit un bloom, à surveiller pour l'eau potable et la baignade.",
    },
  },
  JRC_WATER: {
    usage: [
      "Menu thématique → Eau & humidité → Eaux de surface (JRC).",
      "Donnée statique : occurrence historique de l'eau (%).",
    ],
    example: {
      title: "Séparer eau permanente et saisonnière",
      body: "L'occurrence à 100 % marque un plan d'eau permanent, 20–60 % une zone d'inondation saisonnière — base pour retirer l'eau stable d'une détection de crue.",
    },
  },
  SMAP: {
    date: "2026-06-22",
    usage: [
      "Menu thématique → Eau & humidité → Humidité du sol (SMAP).",
      "Source SMAP (~10 km) et période.",
    ],
    example: {
      title: "Suivre une sécheresse agricole",
      body: "L'humidité de surface SMAP, moyennée sur plusieurs jours, révèle l'installation et la levée d'un déficit hydrique à l'échelle régionale.",
    },
  },
  SNOW: {
    usage: [
      "Menu thématique → Eau & humidité → Couverture neigeuse.",
      "Source MODIS (quotidien) et période.",
    ],
    example: {
      title: "Suivre le manteau neigeux d'un bassin",
      body: "La surface enneigée conditionne la ressource en eau printanière : son recul se lit jour après jour sur MODIS.",
    },
  },

  // ── Climat & température ──
  AIRTEMP: {
    usage: [
      "Menu thématique → Climat & température → Température de l'air (ERA5).",
      "Source ERA5 (~11 km) et période.",
    ],
    example: {
      title: "Comparer air et surface",
      body: "Confronter la température de l'air (ERA5) à la température de surface (LST) éclaire l'écart sol-atmosphère, marqué en ville l'après-midi.",
    },
  },
  PRECIP: {
    date: "2026-06-20",
    usage: [
      "Menu thématique → Climat & température → Précipitations (CHIRPS).",
      "Source CHIRPS (~5 km) et période (cumul).",
    ],
    example: {
      title: "Cumuler la pluie d'une saison",
      body: "CHIRPS additionne les précipitations sur la période choisie : on compare le cumul saisonnier à la normale pour situer une sécheresse.",
    },
  },
  ET: {
    usage: [
      "Menu thématique → Climat & température → Évapotranspiration.",
      "Source MODIS (composite 8 jours) et période.",
      "Valeurs en mm sur la période.",
    ],
    example: {
      title: "Boucler un bilan hydrique agricole",
      body: "L'évapotranspiration mesure l'eau rendue à l'atmosphère : comparée aux précipitations, elle dimensionne les besoins d'irrigation.",
    },
  },
  GPMPRECIP: {
    usage: [
      "Menu thématique → Climat & température → Précipitations (GPM).",
      "Source GPM IMERG (~11 km, mensuel) et période.",
    ],
    example: {
      title: "Cartographier l'intensité des pluies",
      body: "GPM IMERG couvre le globe : utile là où les stations manquent, pour suivre un épisode pluvieux intense.",
    },
  },
  SOLAR: {
    usage: [
      "Menu thématique → Climat & température → Rayonnement solaire.",
      "Source ERA5 (~11 km, mensuel) et période.",
    ],
    example: {
      title: "Évaluer un potentiel photovoltaïque",
      body: "Le rayonnement solaire mensuel classe les sites selon leur gisement, première étape d'une étude d'implantation solaire.",
    },
  },
  WIND: {
    usage: [
      "Menu thématique → Climat & température → Vitesse du vent.",
      "Source ERA5 (~11 km, mensuel) et période.",
    ],
    example: {
      title: "Cibler un gisement éolien",
      body: "La vitesse du vent à 10 m repère les zones ventées ; une étude fine exigera ensuite des mesures à hauteur de moyeu.",
    },
  },

  // ── Qualité de l'air ──
  CO: {
    usage: [
      "Menu thématique → Qualité de l'air → Monoxyde de carbone (CO).",
      "Source Sentinel-5P (~7 km) ; agrégez sur plusieurs jours.",
    ],
    example: {
      title: "Suivre un panache de combustion",
      body: "Le CO trace les grands feux et la combustion incomplète : son panache se déplace au gré des vents sur plusieurs jours.",
    },
  },
  CH4: {
    usage: [
      "Menu thématique → Qualité de l'air → Méthane (CH₄).",
      "Source Sentinel-5P (~7 km) ; agrégez pour lisser.",
    ],
    example: {
      title: "Repérer des sources de méthane",
      body: "Les anomalies de colonne de méthane pointent des sources — sites gaziers, décharges, zones humides — à investiguer plus finement.",
    },
  },
  O3: {
    usage: [
      "Menu thématique → Qualité de l'air → Ozone (O₃).",
      "Source Sentinel-5P (~7 km) et période.",
    ],
    example: {
      title: "Suivre la colonne d'ozone",
      body: "La colonne totale d'ozone varie avec la saison et la latitude ; Sentinel-5P en donne une vue quotidienne à l'échelle continentale.",
    },
  },
  AER: {
    usage: [
      "Menu thématique → Qualité de l'air → Aérosols.",
      "Source Sentinel-5P (~7 km) et période.",
    ],
    example: {
      title: "Cartographier fumées et poussières",
      body: "L'indice d'aérosols absorbants met en évidence les panaches de fumée d'incendie et les intrusions de poussières désertiques.",
    },
  },

  // ── Urbain & aménagement ──
  NDBI: {
    usage: [
      "Menu thématique → Urbain & aménagement → Bâti (NDBI).",
      "Source Sentinel-2 et période.",
    ],
    example: {
      title: "Cartographier l'imperméabilisation",
      body: "Le NDBI ressort sur les surfaces bâties et minéralisées : on suit l'étalement urbain en comparant deux dates.",
    },
  },
  BSI: {
    usage: [
      "Menu thématique → Urbain & aménagement → Sol nu (BSI).",
      "Source Sentinel-2 ou Landsat et période.",
    ],
    example: {
      title: "Repérer chantiers et sols nus",
      body: "Le BSI isole les sols nus et remaniés : utile pour détecter un nouveau chantier ou suivre une reprise de végétation.",
    },
  },
  WORLDCOVER: {
    date: "2026-06-30",
    usage: [
      "Menu thématique → Urbain & aménagement → Occupation du sol (ESA WorldCover).",
      "Donnée statique 2021 (10 m, 11 classes).",
      "Classification discrète : ne pas y appliquer de moyenne.",
    ],
    example: {
      title: "Disposer d'une occupation du sol de référence",
      body: "ESA WorldCover fournit une carte homogène à 10 m — socle pour croiser d'autres couches ou entraîner une classification sur mesure.",
    },
  },
  VIIRS: {
    usage: [
      "Menu thématique → Urbain & aménagement → Lumières nocturnes.",
      "Source VIIRS (500 m) et période.",
    ],
    example: {
      title: "Approcher l'activité humaine de nuit",
      body: "La radiance nocturne sert de proxy d'urbanisation et d'électrification ; sa chute peut signaler une crise ou une panne de réseau.",
    },
  },
  POPULATION: {
    usage: [
      "Menu thématique → Urbain & aménagement → Densité de population.",
      "Source GPW (CIESIN) ou GHSL (JRC), époque la plus récente.",
    ],
    example: {
      title: "Chiffrer la population exposée à un aléa",
      body: "En croisant la densité avec l'emprise d'une inondation ou d'un feu, on estime le nombre d'habitants concernés.",
    },
  },
  BUILT: {
    usage: [
      "Menu thématique → Urbain & aménagement → Surface bâtie (GHSL).",
      "Source GHSL (JRC, 100 m) ; millésimes 1975–2030.",
    ],
    example: {
      title: "Suivre la croissance de l'emprise bâtie",
      body: "Les millésimes GHSL permettent de mesurer l'expansion du bâti d'une ville sur un demi-siècle.",
    },
  },
  SMOD: {
    usage: [
      "Menu thématique → Urbain & aménagement → Degré d'urbanisation (GHSL SMOD).",
      "Source GHSL SMOD (JRC, 1 km) ; millésimes 1975–2030.",
    ],
    example: {
      title: "Distinguer ville, périurbain et rural",
      body: "Le SMOD classe le territoire selon le degré d'urbanisation, cadre officiel pour comparer des dynamiques d'aménagement.",
    },
  },
  DYNWORLD: {
    usage: [
      "Menu thématique → Urbain & aménagement → Occupation du sol temps réel.",
      "Source Dynamic World (Google, 10 m, ~5 j) et période (classe majoritaire).",
    ],
    example: {
      title: "Observer une occupation du sol quasi temps réel",
      body: "Produit par apprentissage profond sur Sentinel-2, Dynamic World actualise l'occupation du sol tous les quelques jours — précieux après un événement.",
    },
  },

  // ── Océans & littoral ──
  BATHY: {
    usage: [
      "Menu thématique → Océans & littoral → Bathymétrie & relief sous-marin.",
      "Donnée ETOPO1 (~1,8 km), statique.",
    ],
    example: {
      title: "Lire le relief d'un plateau continental",
      body: "ETOPO1 combine profondeur des océans et altitude des terres : on y repère plateau, talus et fosses d'un bassin maritime.",
    },
  },
  SSTANOM: {
    usage: [
      "Menu thématique → Océans & littoral → Anomalie de SST.",
      "Source NOAA OISST (~27 km) et période.",
    ],
    example: {
      title: "Détecter une canicule marine",
      body: "L'écart à la normale saisonnière isole les vagues de chaleur océaniques, à risque pour herbiers et coraux — signature aussi d'El Niño.",
    },
  },
  CHLORO: {
    date: "2026-06-26",
    usage: [
      "Menu thématique → Océans & littoral → Chlorophylle-a.",
      "Source MODIS-Aqua ou VIIRS (~4 km) et période.",
    ],
    example: {
      title: "Suivre une efflorescence phytoplanctonique",
      body: "La chlorophylle-a mesure la biomasse phytoplanctonique : ses pics marquent les zones d'upwelling productives et les efflorescences côtières.",
    },
  },
  TURBIDITY: {
    usage: [
      "Menu thématique → Océans & littoral → Matières en suspension (turbidité).",
      "Source MODIS-Aqua (~4 km) et période.",
    ],
    example: {
      title: "Cartographier le panache d'un estuaire",
      body: "La charge particulaire trace le panache turbide d'un fleuve après une crue et sert de proxy au carbone organique côtier.",
    },
  },
  CURRENTS: {
    usage: [
      "Menu thématique → Océans & littoral → Courants marins.",
      "Source modèle HYCOM (~9 km) et période.",
    ],
    example: {
      title: "Visualiser la circulation de surface",
      body: "La vitesse des courants HYCOM éclaire le transport d'eaux, de polluants ou de larves le long d'un littoral.",
    },
  },
  OCEANWIND: {
    usage: [
      "Menu thématique → Océans & littoral → Vent océanique.",
      "Source ERA5 (~31 km, horaire) et période.",
    ],
    example: {
      title: "Suivre le vent au large",
      body: "Le vent à 10 m au-dessus des mers structure l'état de la mer et l'upwelling côtier ; ERA5 en donne l'évolution horaire.",
    },
  },
  SALINITY: {
    usage: [
      "Menu thématique → Océans & littoral → Salinité de surface.",
      "Source modèle HYCOM (~9 km) et période.",
    ],
    example: {
      title: "Repérer un panache d'eau douce",
      body: "La baisse de salinité à l'embouchure d'un fleuve dessine son panache dessalé, moteur de la stratification côtière.",
    },
  },
  SEAICE: {
    usage: [
      "Menu thématique → Océans & littoral → Glace de mer.",
      "Source NOAA OISST (~27 km) et période.",
    ],
    example: {
      title: "Suivre l'étendue de la banquise",
      body: "La concentration de glace (%) trace l'avancée et le retrait saisonnier de la banquise, indicateur climatique majeur.",
    },
  },
  CORAL: {
    usage: [
      "Menu thématique → Océans & littoral → Récifs coralliens.",
      "Donnée Allen Coral Atlas (~5 m), statique.",
    ],
    example: {
      title: "Localiser l'emprise des récifs",
      body: "L'Allen Coral Atlas cartographie les habitats récifaux à fine résolution, base pour le suivi du blanchissement et la protection.",
    },
  },
  MANGROVE: {
    usage: [
      "Menu thématique → Océans & littoral → Mangroves.",
      "Donnée Global Mangrove Forests (30 m), référence 2000.",
    ],
    example: {
      title: "Disposer d'une mangrove de référence",
      body: "L'emprise 2000 sert d'état initial pour mesurer pertes et gains de mangrove, écosystèmes clés du stockage de carbone bleu.",
    },
  },

  // ── Risques & changements ──
  NDSI: {
    usage: [
      "Menu thématique → Risques & changements → Neige (NDSI).",
      "Source Sentinel-2 ou Landsat et période.",
    ],
    example: {
      title: "Cartographier finement la neige",
      body: "Le NDSI sépare neige et nuages là où le visible les confond : utile pour un suivi de manteau neigeux à 10–30 m.",
    },
  },
  BURNED: {
    usage: [
      "Menu thématique → Risques & changements → Zones brûlées.",
      "Source MODIS MCD64A1 (500 m, mensuel) et période.",
      "La bande code le jour de détection, pas une intensité.",
    ],
    example: {
      title: "Dresser le bilan d'une saison de feux",
      body: "Les surfaces brûlées mensuelles cumulées donnent l'étendue totale incendiée d'une saison à l'échelle régionale.",
    },
  },
  FIRMS: {
    usage: [
      "Menu thématique → Risques & changements → Feux actifs.",
      "Source FIRMS (1 km, quasi temps réel) et période.",
    ],
    example: {
      title: "Suivre des foyers actifs en cours",
      body: "FIRMS détecte la chaleur au passage du satellite : un feu éteint entre deux passages n'apparaît pas — à croiser avec les zones brûlées.",
    },
  },

  // ── Relief & 3D / LiDAR ──
  SLOPE: {
    usage: [
      "Menu thématique → Relief & 3D / LiDAR → Pente.",
      "Source Copernicus DEM ou SRTM (30 m), statique ; exagération réglable.",
    ],
    example: {
      title: "Cartographier les zones à risque",
      body: "La pente conditionne érosion, ruissellement et glissements : les fortes pentes ciblent les secteurs à surveiller.",
    },
  },
  HILLSHADE: {
    usage: [
      "Menu thématique → Relief & 3D / LiDAR → Relief ombré.",
      "Source Copernicus DEM ou SRTM (30 m) ; réglez soleil (azimut/hauteur) et exagération.",
    ],
    example: {
      title: "Habiller une carte avec du relief",
      body: "L'ombrage donne du volume au terrain sous les autres couches, améliorant la lecture d'une carte thématique.",
    },
  },
};
