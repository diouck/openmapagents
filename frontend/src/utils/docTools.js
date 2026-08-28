/**
 * docTools.js — Contenu ÉDITORIAL des pages d'OUTILS.
 *
 * Les outils (inondation, bassin versant, dNBR…) ne sont pas dans INDICATOR_DOC :
 * leur titre/catégorie/icône viennent de MENU_TREE, mais leur explication est
 * rédigée ici. Clé = `id` de l'outil dans MENU_TREE.
 *
 * Champs par outil :
 *   abstract  : à quoi ça sert (obligatoire pour une page riche)
 *   dataLine  : la donnée mobilisée (affichée dans le rail, à la place d'un capteur)
 *   usage     : étapes d'utilisation
 *   example   : { title, body, stats?: [{ v, k }] }
 *   caveat    : limites & pièges
 *   date      : ISO — publication (alimente « Récemment publiés »)
 *
 * Un outil de MENU_TREE sans entrée ici obtient quand même sa page (abstract =
 * description du menu, usage générique).
 */
export const DOC_TOOLS = {
  flood: {
    date: "2026-07-23",
    dataLine: "Sentinel-1 SAR · MERIT Hydro (HAND) · GHSL",
    abstract: "Cartographie l'étendue d'une inondation et estime son impact humain. Deux voies : la détection radar Sentinel-1 (avant/après), qui traverse les nuages de la tempête, ou un modèle de hauteur d'eau sur le terrain (HAND). En sortie : la nappe inondée, sa surface, la population et le bâti exposés (GHSL).",
    usage: [
      "Menu thématique → Risques & changements → Cartographie des inondations.",
      "Choisissez le mode : SAR (détection d'un événement réel) ou Modèle MNT (simulation par hauteur d'eau).",
      "En SAR, définissez une fenêtre « avant » (hors crue) et « après » (au pic) ; en modèle, faites glisser le curseur de hauteur d'eau.",
      "Réglez la sensibilité pour coller à l'emprise visible ; lisez surface, population et bâti exposés.",
    ],
    example: {
      title: "Estimer les dégâts d'une crue de la Garonne",
      body: "Sur Marmande, une image Sentinel-1 d'avant-crue comparée à une image au pic révèle la nappe débordée. La détection est recollée en une emprise continue (morphologie), puis croisée avec GHSL pour chiffrer la population et le bâti sous l'eau.",
      stats: [
        { v: "SAR", k: "Traverse les nuages" },
        { v: "GHSL", k: "Population + bâti" },
      ],
    },
    caveat: "L'optique (Sentinel-2) a été retirée : trop dépendante des nuages et aveugle à l'eau turbide. En SAR, l'eau peu profonde ou agitée peut échapper à la détection — montez la sensibilité.",
  },
  watershed: {
    date: "2026-07-09",
    dataLine: "HydroSHEDS (HydroBASINS, réseau) · GLDAS",
    abstract: "Délimite un bassin versant à partir d'un simple point exutoire cliqué sur la carte. L'outil remonte tout l'amont hydrologique (HydroSHEDS), trace le réseau hydrographique et calcule des attributs : surface, périmètre, longueur du réseau, relief, type de sol, précipitations, proxys de nappe et de pérennité des cours d'eau.",
    usage: [
      "Menu thématique → Eau & humidité → Bassin versant.",
      "Choisissez le niveau de délimitation, puis cliquez l'exutoire sur la carte (de préférence sur un cours d'eau).",
      "Le bassin, son réseau et ses attributs se calculent en plusieurs étapes.",
      "Lisez la fiche d'attributs ; exportez la limite ou le réseau si besoin.",
    ],
    example: {
      title: "Caractériser le bassin d'un point de mesure",
      body: "En cliquant à l'exutoire d'une station hydrométrique, on obtient la surface drainée en amont — donnée clé pour rapporter un débit à sa surface contributive et comparer des bassins entre eux.",
    },
    caveat: "La délimitation suit les sous-bassins HydroSHEDS (résolution ~500 m) : un exutoire posé sur le cours principal peut renvoyer un très grand bassin. C'est la bonne réponse hydrologique, pas une erreur.",
  },
  burnsev: {
    date: "2026-07-07",
    dataLine: "Sentinel-2 (NBR avant/après)",
    abstract: "Mesure la sévérité d'un incendie par la différence de l'indice NBR entre une image d'avant-feu et une image d'après-feu (dNBR). Les seuils standard UN-SPIDER classent la surface en degrés de sévérité, de la végétation intacte au brûlis sévère, et en donnent la surface par classe.",
    usage: [
      "Menu thématique → Risques & changements → Sévérité d'incendie (dNBR).",
      "Renseignez la période d'avant-feu, puis d'après-feu (onglets SWIR avant / après pour vérifier les composites).",
      "Choisissez l'emprise (vue, couche vectorielle ou raster).",
      "Lisez la carte de sévérité et les surfaces par classe ; récupérez le périmètre vectorisé.",
    ],
    example: {
      title: "Bilan d'un feu de forêt méditerranéen",
      body: "Deux semaines après un incendie, le dNBR sépare le cœur sévèrement brûlé des lisières modérément touchées. La surface par classe alimente directement le bilan de dégâts.",
    },
    caveat: "Choisir des images peu nuageuses proches de l'événement : un délai trop long laisse la végétation repartir et sous-estime la sévérité.",
  },
  change: {
    dataLine: "Sentinel-2 / Landsat (deux dates)",
    abstract: "Compare la même zone à deux dates et isole ce qui a changé. Utile pour suivre une urbanisation, une déforestation, une mise en eau ou l'emprise d'un chantier, sans se noyer dans les détails stables entre les deux images.",
    usage: [
      "Menu thématique → Risques & changements → Détection de changement.",
      "Choisissez la source, l'indice et les deux dates à comparer.",
      "La couche de changement met en évidence les gains et les pertes.",
    ],
  },
  compare: {
    dataLine: "Deux couches de la carte",
    abstract: "Affiche deux couches côte à côte sous un curseur de balayage, pour comparer deux dates, deux indices ou deux sources exactement au même endroit. La lecture est immédiate : on fait glisser la poignée et l'œil fait le reste.",
    usage: [
      "Menu thématique → Risques & changements → Comparateur A/B.",
      "Désignez la couche de gauche (A) et celle de droite (B).",
      "Faites glisser le curseur pour révéler l'une ou l'autre.",
    ],
  },
  agri: {
    date: "2026-07-06",
    dataLine: "Sentinel-2 (indices, stades)",
    abstract: "Suivi parcellaire pour l'agriculture de précision : sur une parcelle dessinée ou importée, l'outil calcule les indices de végétation dans le temps, situe le stade du couvert et remonte des alertes de stress. C'est le NDVI et ses cousins, mis au service d'une décision agronomique.",
    usage: [
      "Menu thématique → Végétation & agriculture → Agriculture de précision.",
      "Dessinez ou importez la parcelle.",
      "Choisissez la période ; l'outil trace l'évolution des indices et signale les anomalies.",
    ],
  },
  lidar: {
    dataLine: "Nuage de points LAS / LAZ",
    abstract: "Foresterie à partir d'un nuage de points LiDAR aéroporté : l'outil reconstruit le modèle de terrain (MNT), de surface (MNS) et de hauteur de canopée (MNH), détecte les arbres et délimite leurs houppiers. De la donnée brute à l'inventaire, sur une emprise.",
    usage: [
      "Menu thématique → Relief & 3D / LiDAR → LiDAR — foresterie.",
      "Importez un fichier LAS / LAZ.",
      "Lancez le traitement : MNT/MNS/MNH, puis détection des arbres et houppiers.",
    ],
    caveat: "La qualité dépend de la densité de points du levé : un nuage clairsemé sous-estime les petits arbres et fusionne les houppiers voisins.",
  },
  profil: {
    dataLine: "Copernicus DEM / SRTM (30 m)",
    abstract: "Trace le profil altimétrique le long d'un tracé dessiné sur la carte : dénivelé, pentes et forme du relief se lisent d'un coup d'œil. Pratique pour préparer un itinéraire, un aménagement ou une coupe de terrain.",
    usage: [
      "Menu thématique → Relief & 3D / LiDAR → Profil altimétrique.",
      "Dessinez la ligne du profil sur la carte.",
      "Lisez la coupe altimétrique et le dénivelé cumulé.",
    ],
  },
  scene3d: {
    dataLine: "Bâtiments 3D · 3D Tiles · nuages de points",
    abstract: "Bascule la scène en 3D : bâtiments extrudés, tuiles 3D, nuages de points et globe. Pour lire une ville en volume, présenter une maquette ou explorer un relief en perspective, au-delà de la carte plane.",
    usage: [
      "Menu thématique → Urbain & aménagement → Bâtiments 3D / Globe.",
      "Activez les bâtiments 3D, une couche 3D Tiles ou un nuage de points.",
      "Naviguez en perspective ; combinez avec le relief et l'ambiance de l'en-tête.",
    ],
  },
  isochrone: {
    dataLine: "Réseau routier (Mapbox)",
    abstract: "Calcule les zones atteignables depuis un point en un temps donné, à pied, à vélo ou en voiture. L'isochrone matérialise l'accessibilité réelle — celle du réseau, pas du vol d'oiseau — pour une étude de desserte ou d'implantation.",
    usage: [
      "Menu thématique → Urbain & aménagement → Isochrone d'accès.",
      "Placez le point de départ et choisissez le mode et la durée.",
      "L'isochrone s'affiche ; superposez-y d'autres couches (population, équipements).",
    ],
  },
  route: {
    dataLine: "Réseau routier (Mapbox)",
    abstract: "Calcule l'itinéraire, la distance et le temps de parcours entre deux points, à pied, à vélo ou en voiture. Le tracé suit le réseau réel et se dépose comme une couche exploitable.",
    usage: [
      "Menu thématique → Urbain & aménagement → Itinéraire.",
      "Placez le départ et l'arrivée, choisissez le mode.",
      "Lisez la distance et la durée ; l'itinéraire devient une couche.",
    ],
  },
  timelapse: {
    dataLine: "Séries GEE (Sentinel / Landsat / MODIS)",
    abstract: "Génère un timelapse animé d'une série d'images satellite sur une emprise : la même scène, image après image, révèle une dynamique — étalement urbain, retrait d'un glacier, assèchement d'un lac. Le rendu est produit côté serveur (GIF).",
    usage: [
      "Menu thématique → Imagerie satellite (ou Climat) → Timelapse.",
      "Choisissez la source, l'emprise et la plage d'années.",
      "Lancez la génération ; récupérez l'animation.",
    ],
    caveat: "À distinguer du timelapse « sur la carte » (animation des tuiles dans MapLibre) : ici le rendu est un GIF produit sur le serveur.",
  },
  vectorcat: {
    date: "2026-07-12",
    dataLine: "GEE · APIs ouvertes (GBIF, iNaturalist, geoBoundaries, GDACS…)",
    abstract: "Un catalogue pour chercher et ajouter des données vectorielles sans quitter la carte : limites administratives, routes, rivières, aires protégées, séismes, biodiversité… Les données arrivent brutes, sans sémiologie imposée — vous choisissez ensuite le style et l'analyse. L'aspect temporel est préservé et les données restent exploitables en tableau.",
    usage: [
      "Menu thématique → Outils & données → Catalogue vectoriel.",
      "Recherchez une donnée ; l'outil interroge GEE et des APIs ouvertes.",
      "Ajoutez la couche : elle est découpée sur l'emprise et la carte zoome dessus.",
      "Stylez et analysez librement (le catalogue n'impose rien).",
    ],
  },
  projections: {
    date: "2026-07-11",
    dataLine: "d3-geo · fonds Natural Earth",
    abstract: "Un composeur de carte qui sort de Mercator : Robinson, Peters, Mollweide, Spilhaus… pour montrer que toute projection est un choix qui déforme quelque chose. On y règle la projection, l'habillage et la sémiologie, puis on exporte en PNG ou PDF.",
    usage: [
      "Menu thématique → Outils & données → Projections du monde.",
      "Choisissez la projection et l'habillage (grille, fonds, titres).",
      "Ajoutez vos couches et réglez la sémiologie.",
      "Exportez en PNG ou PDF.",
    ],
    caveat: "Vue de composition hors carte live : sous MapLibre, seules les projections Plan (Mercator) et Globe existent.",
  },
  bivariate: {
    dataLine: "Deux variables d'une même couche",
    abstract: "Croise deux variables dans une seule carte à l'aide d'une palette bivariée 3×3 : on lit d'un coup les zones fortes sur l'une, l'autre, ou les deux. Idéal pour révéler une relation spatiale — par exemple revenu et densité, ou température et végétation.",
    usage: [
      "Menu thématique → Outils & données → Carte bivariée.",
      "Choisissez la couche et les deux variables à croiser.",
      "Réglez les classes ; la légende 3×3 explique la lecture.",
    ],
  },
  classif: {
    dataLine: "Vos échantillons + une image satellite",
    abstract: "Classification supervisée : à partir d'échantillons que vous désignez (eau, forêt, bâti…), l'outil entraîne un modèle et étend ces classes à toute l'image. C'est la fabrique d'une carte d'occupation du sol sur mesure, adaptée à votre zone et vos classes.",
    usage: [
      "Menu thématique → Outils & données → Classification supervisée.",
      "Créez des échantillons par classe sur la carte.",
      "Entraînez le modèle et appliquez-le ; consultez les métriques de qualité.",
    ],
    caveat: "La qualité dépend d'échantillons représentatifs et équilibrés : trop peu d'exemples ou des classes déséquilibrées biaisent le résultat.",
  },
  story: {
    date: "2026-08-28",
    dataLine: "Vos couches + vues de caméra · MapLibre GL JS",
    abstract: "Un atelier de « story map » : on raconte une histoire en chapitres, chacun mémorisant une vue de la carte (centre, zoom, inclinaison, orientation) capturée d'un clic, un texte et les couches visibles. « Lire » enchaîne les vues dans l'application ; « Exporter en HTML » produit un fichier autonome où, au défilement, la carte vole d'un chapitre à l'autre — le scrollytelling classique, prêt à partager.",
    usage: [
      "Menu thématique → Outils & données → Story map (scrollytelling).",
      "Cadrez la carte sur la première scène, puis « + Chapitre (vue actuelle) » : la vue et les couches visibles sont mémorisées.",
      "Rédigez le texte de chaque chapitre ; réordonnez, recapturez ou supprimez au besoin.",
      "« Lire » pour prévisualiser dans l'appli ; « Exporter en HTML » pour récupérer un fichier .html à faire défiler et diffuser.",
    ],
    example: {
      title: "Présenter l'évolution d'un territoire en trois temps",
      body: "Chapitre 1 : vue large sur la région. Chapitre 2 : zoom sur une ville avec la couche d'occupation du sol. Chapitre 3 : bascule en 3D inclinée sur un quartier. À l'export, le lecteur fait défiler et la carte enchaîne les trois plans toute seule.",
      stats: [
        { v: "HTML", k: "fichier autonome partageable" },
        { v: "Scroll", k: "vols de caméra au défilement" },
      ],
    },
    caveat: "L'export embarque vos couches (GeoJSON en clair, aperçus raster en image) et charge MapLibre GL JS + le fond OpenStreetMap depuis Internet : le fichier s'ouvre donc en ligne, et de gros aperçus raster l'alourdissent. La prévisualisation « Lire » anime la carte de l'appli sans modifier vos couches.",
  },
  stac: {
    date: "2026-08-28",
    dataLine: "STAC · Earth Search (Element84) · COG Sentinel-2",
    abstract: "Un navigateur de scènes satellite : on interroge un catalogue STAC (Earth Search) sur l'emprise affichée, filtré par collection, période et couverture nuageuse, et on récupère une liste de scènes triées du plus clair au plus nuageux. Ajouter une scène lit son Cloud-Optimized GeoTIFF (COG) à la volée — via les aperçus internes du fichier, sans le télécharger entièrement — et la superpose à la carte, reprojetée pour coller au fond.",
    usage: [
      "Menu thématique → Outils & données → Navigateur STAC / COG.",
      "Choisissez la collection (Sentinel-2 L2A par défaut), la période et le seuil de nuages.",
      "Cadrez la zone voulue sur la carte, puis « Rechercher dans la vue ».",
      "Parcourez les vignettes ; « Ajouter à la carte » superpose l'aperçu COG de la scène (RVB).",
    ],
    example: {
      title: "Trouver l'image Sentinel-2 la moins nuageuse de l'été",
      body: "Sur une zone donnée, une recherche entre juin et août avec un seuil de 20 % de nuages renvoie les scènes classées par clarté. La première, quasi sans nuage, s'ajoute en un clic — son COG est lu en aperçu (overviews) et reprojeté en Web Mercator.",
      stats: [
        { v: "COG", k: "lecture partielle par plage HTTP" },
        { v: "Sans clé", k: "Earth Search (Element84)" },
      ],
    },
    caveat: "Source : Earth Search (Element84), publique et sans clé. L'ajout à la carte n'est disponible que pour les collections avec un COG RVB « visual » (Sentinel-2) ; les aperçus sont downsamplés (~1024 px) pour rester légers. Par sécurité, seuls les hôtes d'assets whitelistés (bucket public sentinel-cogs) sont lus côté serveur.",
  },
  solarsystem: {
    date: "2026-08-28",
    dataLine: "Textures Solar System Scope (CC-BY) · Three.js",
    abstract: "Un petit planétarium : chaque corps du système solaire — du Soleil à Neptune, plus la Lune — s'affiche en sphère 3D texturée que l'on fait tourner et zoomer. Saturne porte ses anneaux. Là où la carte ne peut montrer que les corps disposant de tuiles (Mercure, Mars, Lune), ce viewer couvre tout le système grâce à des textures équirectangulaires plaquées sur une sphère.",
    usage: [
      "Menu thématique → Planètes → cliquez un corps (Soleil, planètes, Lune) ou « Système solaire (vue 3D) ».",
      "Le corps s'affiche en grand à la place de la carte ; changez de corps dans la liste déroulante.",
      "Glissez pour tourner le globe, la molette pour zoomer ; il tourne seul.",
      "« Retour à la carte » (ou ouvrir un autre module / ajouter une donnée) revient automatiquement à la carte.",
    ],
    example: {
      title: "Comparer Jupiter et une planète tellurique",
      body: "On passe de la Terre à Jupiter d'un clic : les bandes nuageuses de la géante gazeuse, impossibles à afficher via un fond de carte classique (pas de tuiles), s'affichent ici en relief texturé.",
      stats: [
        { v: "Soleil→Neptune", k: "+ Lune, anneaux de Saturne" },
        { v: "3D", k: "sphère texturée Three.js" },
      ],
    },
    caveat: "Complément au sélecteur de planète de la carte (qui, lui, n'a que Mercure/Mars/Lune faute de tuiles). Les textures (Solar System Scope, CC-BY 4.0) sont embarquées dans l'application (frontend) : elles s'affichent sans backend ni accès Internet du serveur.",
  },
  georef: {
    date: "2026-08-28",
    dataLine: "Image (plan scanné, photo) + points d'appui · scipy",
    abstract: "Cale une image sans coordonnées — un plan ancien scanné, une photo aérienne, un croquis — sur la carte. On désigne des points d'appui (GCP) : un repère cliqué sur l'image, puis le même lieu cliqué sur la carte. À partir de ces correspondances, l'outil ajuste une transformation (affine ou projective), reprojette l'image en Web Mercator et la superpose au bon endroit, avec une erreur de calage (RMSE) pour juger la qualité.",
    usage: [
      "Menu thématique → Analyse → Géoréférenceur, puis importez l'image.",
      "Cliquez un repère identifiable sur l'image (un carrefour, un angle).",
      "« Placer sur la carte » puis cliquez ce même lieu sur la carte : le point d'appui est enregistré.",
      "Répétez (≥3 pour affine, ≥4 pour projective, bien répartis), puis « Géoréférencer » — l'image calée s'ajoute comme couche.",
    ],
    example: {
      title: "Superposer un plan cadastral ancien",
      body: "En pointant quatre carrefours reconnaissables sur le plan scanné puis sur la carte actuelle, une transformation projective redresse le plan et le pose sur le fond moderne — un RMSE de quelques mètres confirme un bon calage.",
      stats: [
        { v: "Affine / Projective", k: "3 ou 4 points minimum" },
        { v: "RMSE", k: "erreur de calage en mètres" },
      ],
    },
    caveat: "La précision dépend du nombre de points d'appui et de leur répartition (évitez de les aligner ou de les regrouper). L'affine gère rotation/échelle/cisaillement ; la projective corrige en plus la perspective (photo oblique). Image jusqu'à 5 000 px de côté.",
  },
  vectorviz: {
    date: "2026-08-28",
    dataLine: "Vos couches de points · MapLibre",
    abstract: "Deux façons de révéler la structure d'un semis de points. La carte de chaleur (densité KDE) dessine un dégradé continu qui fait ressortir les zones de concentration ; le regroupement (clusters) condense les points proches en pastilles chiffrées qui se scindent au zoom, pour afficher des milliers de points sans surcharge.",
    usage: [
      "Menu thématique → Analyse → Chaleur & clusters.",
      "Choisissez une couche de points et le mode (chaleur ou clusters).",
      "Réglez le rayon (et l'intensité pour la chaleur), puis créez la couche.",
    ],
    example: {
      title: "Voir les foyers d'un semis d'observations",
      body: "À partir de milliers de points d'observation, la carte de chaleur fait ressortir d'un coup les foyers de concentration ; en clusters, on garde chaque point tout en gardant la carte lisible au dézoom.",
      stats: [
        { v: "KDE", k: "densité continue" },
        { v: "Clusters", k: "regroupement au zoom" },
      ],
    },
    caveat: "Rendu natif MapLibre, calculé côté navigateur (sans backend). Pour l'agrégation en hexagones, la densité sur grille ou les flux origine→destination, utilisez l'outil Analyse spatiale (groupes Géométrie, Statistiques et Mobilité/Flux) — non dupliqués ici.",
  },
  viewshed: {
    date: "2026-08-28",
    dataLine: "MNT mondial ~30 m (Terrarium AWS) · numpy",
    abstract: "Répond à « qu'est-ce qu'on voit d'ici ? ». Depuis un observateur placé sur la carte, l'outil reconstruit le relief autour (modèle numérique de terrain mondial) et calcule, par lancer de rayons, les zones visibles en tenant compte des obstacles du terrain, de la hauteur de l'observateur et — au choix — de la courbure terrestre. Le résultat est une nappe verte de visibilité, prête à croiser avec d'autres couches.",
    usage: [
      "Menu thématique → Analyse → Analyse de visibilité.",
      "« Placer l'observateur », puis cliquez le point de vue sur la carte.",
      "Réglez la hauteur de l'observateur (piéton, tour, drone…), le rayon et la hauteur de cible.",
      "« Calculer la visibilité » : la zone visible s'ajoute en overlay + la surface visible.",
    ],
    example: {
      title: "Portée visuelle d'un point de vue",
      body: "Depuis un belvédère, on visualise d'un coup les versants vus et cachés dans un rayon de 5 km ; en montant la hauteur de l'observateur (une tour), l'emprise visible s'étend nettement.",
      stats: [
        { v: "Rayons", k: "angle d'élévation cumulé" },
        { v: "Sans clé", k: "MNT Terrarium mondial" },
      ],
    },
    caveat: "Le MNT est global (~30 m) : il lisse les sommets pointus, donc sur relief marqué un observateur bas (2 m) peut « buter » sur la brisure de pente proche — augmentez sa hauteur. Ni la végétation ni le bâti ne sont modélisés. Le rayon est borné (zone téléchargée) ; réduisez-le si la zone est trop grande.",
  },
  spatialstats: {
    date: "2026-08-28",
    dataLine: "Couche vecteur + champ numérique · numpy",
    abstract: "Mesure si un phénomène est spatialement structuré. L'indice de Moran répond à la question globale — les valeurs voisines se ressemblent-elles (agrégation), s'opposent-elles (dispersion) ou sont-elles aléatoires ? — avec un test par permutations. Les hotspots de Getis-Ord (Gi*) descendent au local : chaque entité reçoit un z-score qui la classe en point chaud (amas de fortes valeurs) ou froid, à 90/95/99 % de confiance.",
    usage: [
      "Menu thématique → Analyse → Stats spatiales.",
      "Choisissez une couche (points ou polygones) et un champ numérique.",
      "Réglez le nombre de voisins (k) ; « Analyser » calcule Moran et les Gi*.",
      "Lisez l'indice de Moran et son verdict ; « Ajouter la couche » ajoute les entités enrichies (gi_class), à colorer dans le gestionnaire de couches.",
    ],
    example: {
      title: "Repérer les amas de forte valeur foncière",
      body: "Sur des parcelles portant un prix au m², Moran confirme d'abord une agrégation significative ; les Gi* localisent ensuite les quartiers « points chauds » (prix élevés entourés de prix élevés) et « points froids », prêts à cartographier en rouge/bleu.",
      stats: [
        { v: "Moran I", k: "autocorrélation globale + p" },
        { v: "Gi*", k: "points chauds/froids locaux" },
      ],
    },
    caveat: "Voisinage = k plus proches voisins sur les centroïdes (marche pour points et polygones) ; un k différent change les résultats. Le champ doit être numérique et non constant. Limité à 3 000 entités (test par permutations). Sans dépendance PySAL : calcul maison numpy.",
  },
  rastervec: {
    date: "2026-08-28",
    dataLine: "GeoTIFF mono-bande importé · rasterio · scikit-image",
    abstract: "Transforme un raster importé en couche vecteur, de deux façons. La vectorisation en polygones découpe le raster en classes d'intervalles égaux et trace le contour de chaque zone (une carte d'occupation, des paliers d'altitude ou de pente). Les courbes de niveau tracent des isolignes à valeurs régulières — le rendu classique d'un modèle de terrain. Le résultat est une couche vecteur ordinaire : stylable, interrogeable, exportable.",
    usage: [
      "Importez un GeoTIFF mono-bande, puis Menu thématique → Analyse → Vectorisation raster.",
      "Onglet Polygones : choisissez le nombre de classes, puis « Vectoriser » — chaque polygone porte sa classe et ses bornes (min/max).",
      "Onglet Contours : fixez un nombre de niveaux ou un intervalle (ex. 10 m), puis « Générer les contours » — chaque ligne porte sa valeur.",
      "La couche s'ajoute à la carte ; stylez-la ou exportez-la comme toute couche vecteur.",
    ],
    example: {
      title: "Tirer des courbes de niveau et des paliers d'un MNT",
      body: "Sur un modèle de terrain importé, un intervalle de 10 m produit les courbes de niveau topographiques ; en parallèle, une vectorisation en 6 classes délimite les grands paliers d'altitude en polygones exploitables.",
      stats: [
        { v: "Polygones", k: "par classe d'intervalle" },
        { v: "Isolignes", k: "niveaux ou intervalle fixe" },
      ],
    },
    caveat: "Rasters mono-bande uniquement. Sortie bornée (≈20 000 polygones / 300 000 sommets) : réduisez le nombre de classes ou augmentez l'intervalle si le résultat est tronqué. Les contours remplissent les zones sans donnée par le minimum, ce qui peut créer une isoligne au bord du nodata.",
  },
  rasteranalysis: {
    date: "2026-08-28",
    dataLine: "GeoTIFF mono-bande importé · DuckDB/numpy · rasterio",
    abstract: "Deux analyses sur un raster importé (altitude, indice, température…). Les statistiques zonales agrègent les pixels par polygone d'une couche vecteur — nombre, min, moyenne, max, écart-type, somme — et enrichissent les zones pour la carte. La calculatrice applique une expression de « map algebra » (A désigne le raster) et produit une nouvelle couche : seuillage, normalisation, reclassement, masquage.",
    usage: [
      "Importez d'abord un GeoTIFF mono-bande (glisser-déposer ou Import), puis ouvrez Menu thématique → Outils & données → Analyse raster.",
      "Statistiques zonales : choisissez le raster et une couche de polygones, puis « Calculer » ; ajoutez les zones enrichies (zs_mean, zs_max…) à la carte.",
      "Calculatrice : choisissez le raster et écrivez une expression avec A (ex. where(A > 0, 1, 0), clip(A, 0, 100), (A - min) / (max - min)).",
      "Le résultat de la calculatrice devient une nouvelle couche image, restylable comme un raster GEE.",
    ],
    example: {
      title: "Extraire les pentes fortes et les compter par commune",
      body: "Sur un MNT importé, la calculatrice binarise le raster (where(A > seuil, 1, 0)) ; les statistiques zonales, croisées avec une couche de communes, donnent alors la somme de pixels « pente forte » et la moyenne par commune, directement en tableau et sur la carte.",
      stats: [
        { v: "6 stats", k: "count · min · moy · max · std · somme" },
        { v: "Sûr", k: "expression sans exécution de code" },
      ],
    },
    caveat: "Rasters mono-bande uniquement (les imports RGB 3 bandes ne sont pas mis en cache). La calculatrice n'autorise que A, des nombres et une liste de fonctions (where, clip, log, sqrt…) — aucun accès système. Le raster importé expire côté serveur après 1 h : réimportez-le au besoin.",
  },
  stats: {
    dataLine: "Une couche de la carte",
    abstract: "Statistiques descriptives d'une couche : min, moyenne, médiane, max, écart-type et histogramme sur les valeurs d'un indice ou d'un attribut. La première étape pour choisir des seuils de classification ou vérifier une donnée.",
    usage: [
      "Menu thématique → Outils & données → Statistiques.",
      "Choisissez la couche (raster ou vectorielle) et le champ.",
      "Lisez les statistiques et l'histogramme sur l'emprise.",
    ],
  },
  layers: {
    abstract: "Le gestionnaire de couches : l'endroit où l'on règle l'ordre d'empilement, le style, l'opacité et la visibilité de chaque couche, et d'où l'on exporte. C'est le tableau de bord de la carte, une fois les données ajoutées.",
    usage: [
      "Menu thématique → Outils & données → Gestionnaire de couches.",
      "Réordonnez, masquez, réglez l'opacité ou le style d'une couche.",
      "Exportez une couche vectorielle au format voulu (GeoJSON, GeoPackage, Shapefile…).",
    ],
  },
  join: {
    dataLine: "Une couche vectorielle + un CSV",
    abstract: "La jointure attributaire rapatrie les colonnes d'un tableau (CSV) vers les entités d'une couche, en s'appuyant sur une clé commune (code commune, identifiant…). C'est le pont entre une géométrie et des données tabulaires externes.",
    usage: [
      "Menu thématique → Outils & données → Jointure attributaire.",
      "Importez le CSV et désignez la couche cible.",
      "Choisissez la clé de correspondance ; les colonnes rejoignent la couche.",
    ],
    caveat: "La clé doit être écrite à l'identique des deux côtés (mêmes codes, mêmes types) — un zéro initial perdu suffit à casser la jointure.",
  },
  spatial: {
    dataLine: "Deux couches vectorielles",
    abstract: "L'analyse spatiale croise des couches par la géométrie : intersections, jointures spatiales, agrégations par zone. On répond à « qu'y a-t-il dans » ou « combien par » sans clé attributaire, juste par la position.",
    usage: [
      "Menu thématique → Outils & données → Analyse spatiale.",
      "Choisissez l'opération et les couches en entrée.",
      "La couche résultat s'ajoute à la carte.",
    ],
  },
  editor: {
    abstract: "L'éditeur vectoriel pour dessiner et modifier des entités à la main : tracer un polygone d'emprise, corriger une limite, ajouter des points. La donnée créée devient une couche comme les autres.",
    usage: [
      "Menu thématique → Outils & données → Éditeur vectoriel.",
      "Dessinez points, lignes ou polygones ; éditez les sommets.",
      "Renseignez les attributs, puis exportez si besoin.",
    ],
  },
  database: {
    dataLine: "DuckDB (Overture, imports)",
    abstract: "Un accès à la base DuckDB : interroger de grands jeux (Overture Maps notamment) en SQL et rapatrier le résultat comme couche. Pour aller au-delà des catalogues guidés et composer sa propre requête.",
    usage: [
      "Menu thématique → Outils & données → Base de données.",
      "Écrivez la requête ou importez un fichier.",
      "Le résultat s'ajoute à la carte comme couche vectorielle.",
    ],
  },
  sql: {
    date: "2026-08-27",
    dataLine: "DuckDB spatial · vos couches vecteur",
    abstract: "Un éditeur SQL spatial qui prend vos couches vecteur déjà chargées comme tables et les interroge avec toute la boîte à outils géométrique de DuckDB (ST_Buffer, ST_Centroid, ST_Area, ST_Intersects…). Le résultat s'affiche en tableau et, dès qu'il contient une géométrie, s'ajoute à la carte comme une nouvelle couche.",
    usage: [
      "Menu thématique → Outils & données → SQL Workspace.",
      "Chaque couche vecteur chargée devient une table : cliquez son nom pour insérer une requête d'aperçu (le nom SQL est le nom de couche nettoyé en alphanumérique).",
      "Écrivez votre requête (Ctrl/⌘ + Entrée pour l'exécuter) ou partez d'un exemple ; référez les colonnes géométriques sous le nom geom.",
      "Lisez le tableau ; si le résultat a une géométrie, « Ajouter à la carte » en fait une couche exploitable.",
    ],
    example: {
      title: "Générer des tampons de 500 m autour de points",
      body: "Sur une couche de points chargée, SELECT *, ST_Buffer(geom, 0.005) AS geom FROM \"ma_couche\" renvoie les zones d'influence sous forme de polygones, ajoutables à la carte en un clic — sans passer par un SIG lourd.",
      stats: [
        { v: "DuckDB", k: "SQL spatial en mémoire" },
        { v: "ST_*", k: "Fonctions géométriques" },
      ],
    },
    caveat: "Les requêtes portent sur les couches chargées, pas sur des fichiers du serveur : par sécurité (application publique) l'accès externe de DuckDB est verrouillé avant d'exécuter votre SQL. Les résultats sont bornés (5 000 lignes) et la géométrie attendue est en WGS84 (degrés) — un tampon se règle donc en degrés, pas en mètres.",
  },
  ogc: {
    dataLine: "Flux WMS / WFS / WMTS externes",
    abstract: "Le connecteur aux services géographiques standards : on branche un flux WMS, WFS ou WMTS d'un fournisseur (IGN, PDOK, services régionaux…) et sa donnée s'affiche dans la carte, sans import de fichier.",
    usage: [
      "Menu thématique → Outils & données → Services OGC / WMS.",
      "Collez l'URL du service et choisissez la couche proposée.",
      "La couche distante s'ajoute à la carte.",
    ],
    caveat: "La disponibilité et les performances dépendent du serveur distant ; un service surchargé ou hors ligne renverra des tuiles vides.",
  },
  osm: {
    dataLine: "OpenStreetMap (Overpass)",
    abstract: "L'import OpenStreetMap par thème : bâtiments, routes, commerces, cours d'eau… On extrait sur l'emprise les objets d'une catégorie et on les récupère comme couche vectorielle exploitable.",
    usage: [
      "Menu thématique → Outils & données → Import OSM.",
      "Choisissez le thème et l'emprise.",
      "Les objets OSM s'ajoutent en couche, prêts à styler ou analyser.",
    ],
  },
  measure_dist: {
    abstract: "Mesure la longueur d'une ligne tracée sur la carte — un trajet, une façade, un linéaire de réseau. Réponse immédiate, sans créer de couche.",
    usage: [
      "Menu thématique → Outils & données → Mesure distance.",
      "Cliquez les points successifs du tracé.",
      "Lisez la longueur cumulée.",
    ],
  },
  measure_area: {
    abstract: "Mesure la superficie d'un polygone tracé sur la carte — une parcelle, une emprise, une zone d'étude. Lecture directe de la surface.",
    usage: [
      "Menu thématique → Outils & données → Mesure surface.",
      "Tracez le polygone à mesurer.",
      "Lisez la superficie.",
    ],
  },
  buffer: {
    abstract: "Crée une zone tampon autour d'une entité : la bande d'influence à distance fixe d'une route, d'une rivière, d'un point. Base de nombreuses analyses de proximité et de servitudes.",
    usage: [
      "Menu thématique → Outils & données → Zone tampon.",
      "Choisissez l'entité et la distance.",
      "La zone tampon s'ajoute comme couche.",
    ],
  },
  draw: {
    abstract: "Un croquis libre sur la carte : points, lignes et polygones pour annoter, délimiter une zone d'intérêt ou préparer une emprise. Rapide, sans contrainte d'attributs.",
    usage: [
      "Menu thématique → Outils & données → Dessin libre.",
      "Dessinez librement sur la carte.",
      "Réutilisez le tracé comme emprise dans d'autres outils.",
    ],
  },
  gee: {
    dataLine: "Catalogue Google Earth Engine",
    abstract: "Le mode avancé Earth Engine : un accès direct au catalogue GEE pour composer un jeu, un indice ou une période au-delà des indicateurs pré-réglés. Pour l'utilisateur qui sait ce qu'il cherche dans le catalogue.",
    usage: [
      "Menu thématique → Outils & données → GEE — mode avancé.",
      "Choisissez le jeu, l'indice, la période et l'emprise.",
      "La couche calculée s'ajoute à la carte.",
    ],
  },
  weather: {
    date: "2026-07-25",
    dataLine: "RainViewer (radar + IR) · GFS via GEE",
    abstract: "Le temps réel, animé : radar de précipitation, satellite infrarouge (nuages) et prévision du modèle GFS réunis sur une seule frise temporelle passé→futur. On déroule le temps avec un curseur ; chaque couche affiche sa trame la plus proche de l'instant choisi.",
    usage: [
      "Menu thématique → Météo & temps réel → Météo temps réel.",
      "Activez les couches : radar, satellite IR, prévision GFS (température, pluie ou vent).",
      "Utilisez la frise en bas de l'écran : ▶ pour animer, glissez le curseur, réglez la vitesse.",
      "Le mode direct récupère les nouvelles trames toutes les ~5 min.",
    ],
    example: {
      title: "Suivre l'arrivée d'un orage, puis sa prévision",
      body: "On remonte 2 h de radar pour voir la cellule approcher, on franchit le repère « Maintenant », et la frise bascule sur la prévision GFS pour anticiper les prochaines heures.",
    },
    caveat: "Radar et IR viennent de RainViewer (mondial, sans clé) ; la prévision de GFS via GEE (pas horaire, jusqu'à +48 h). Le géostationnaire est une mosaïque IR mondiale, pas un satellite régional à pleine résolution.",
  },
};
