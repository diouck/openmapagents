"""
rag/tool_registry.py — Registre des tools MCP pour OpenMapAgents
=================================================================
Source de vérité unique pour tous les tools disponibles.
Utilisé par embedder.py pour créer les vecteurs pgvector
et par retriever.py pour le boost de score par trigger exact.

Chaque entry contient :
  - server      : nom du MCP server
  - tool        : nom du tool
  - description : description fr+en pour l'embedding
  - triggers    : mots-clés qui déclenchent ce tool (boost exact match)
  - output_action : action retournée au frontend
  - few_shot    : exemples input → tool call
  - params      : paramètres clés du tool
"""

# ═══════════════════════════════════════════════════════════════
# REGISTRE COMPLET — 74 tools sur 10 MCP servers
# ═══════════════════════════════════════════════════════════════

TOOL_REGISTRY: list[dict] = [

    # ══════════════════════════════════════════════════════════
    # GEE — INDICES SPECTRAUX
    # ══════════════════════════════════════════════════════════
    {
        "id":          "gee_ndvi",
        "server":      "gee",
        "tool":        "compute_ndvi",
        "description": (
            "Calcule l'indice de végétation NDVI (Normalized Difference Vegetation Index) "
            "sur une zone géographique à partir de Sentinel-2 ou Landsat. "
            "Visualise la densité et la santé de la végétation. "
            "NDVI vegetation index Sentinel-2 Landsat satellite."
        ),
        "triggers": [
            "ndvi","végétation","vegetation","indice végétation",
            "couverture végétale","sentinel","sentinel-2","landsat",
            "santé des plantes","densité végétale","verdure",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "carte NDVI 2024 sur Dakar", "tool": "compute_ndvi",
             "params": {"bbox":[-17.55,14.63,-17.33,14.82],
                        "start_date":"2024-01-01","end_date":"2024-12-31",
                        "collection":"sentinel2"}},
            {"user": "végétation autour de Nantes", "tool": "compute_ndvi",
             "params": {"bbox":[-1.72,47.15,-1.42,47.32],
                        "start_date":"2024-06-01","end_date":"2024-09-01"}},
        ],
    },
    {
        "id":          "gee_rgb",
        "server":      "gee",
        "tool":        "compute_rgb",
        "description": (
            "Génère un composite RGB vrai couleur depuis Sentinel-2 ou Landsat. "
            "Image satellite couleur naturelle de la zone. "
            "True color satellite image composite RGB."
        ),
        "triggers": [
            "rgb","vraie couleur","vrai couleur","image satellite",
            "photo satellite","composite","couleur naturelle",
            "sentinel rgb","landsat couleur",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "image satellite de Paris", "tool": "compute_rgb",
             "params": {"bbox":[2.22,48.81,2.47,48.90],
                        "start_date":"2024-07-01","end_date":"2024-09-01"}},
        ],
    },
    {
        "id":          "gee_evi",
        "server":      "gee",
        "tool":        "compute_evi",
        "description": (
            "Calcule l'EVI (Enhanced Vegetation Index), meilleur que le NDVI "
            "pour les zones forestières denses. Réduit les effets atmosphériques. "
            "EVI enhanced vegetation index forest dense canopy."
        ),
        "triggers": [
            "evi","enhanced vegetation","forêt dense","canopée dense",
            "végétation dense","tropical","amazonien",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "EVI forêt tropicale", "tool": "compute_evi",
             "params": {"bbox":[-5.0,4.0,1.0,8.0],
                        "start_date":"2024-01-01","end_date":"2024-12-31"}},
        ],
    },
    {
        "id":          "gee_ndwi",
        "server":      "gee",
        "tool":        "compute_ndwi",
        "description": (
            "Calcule le NDWI (Normalized Difference Water Index) pour détecter "
            "l'eau et l'humidité du sol. Fleuves, lacs, zones inondées. "
            "NDWI water index flood detection humidity rivers lakes."
        ),
        "triggers": [
            "ndwi","eau","water","humidité","inondation","flood",
            "lac","rivière","zone humide","cours d'eau","irrigation",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "détecter les inondations", "tool": "compute_ndwi",
             "params": {"start_date":"2024-01-01","end_date":"2024-06-01"}},
        ],
    },
    {
        "id":          "gee_savi",
        "server":      "gee",
        "tool":        "compute_savi",
        "description": (
            "Calcule le SAVI (Soil Adjusted Vegetation Index) pour les zones "
            "semi-arides avec sol nu visible. Sahel, zones arides, agriculture. "
            "SAVI soil adjusted vegetation arid semi-arid Sahel agriculture."
        ),
        "triggers": [
            "savi","sol nu","aride","semi-aride","sahel","sahara",
            "désert","zone sèche","agriculture aride",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "végétation zone sahélienne", "tool": "compute_savi",
             "params": {"start_date":"2024-06-01","end_date":"2024-09-01",
                        "L": 0.5}},
        ],
    },
    {
        "id":          "gee_timelapse",
        "server":      "gee",
        "tool":        "compute_timelapse",
        "description": (
            "Génère un timelapse satellite (animation temporelle) sur plusieurs "
            "années. NDVI, RGB, EVI, NDWI ou SAR. Évolution temporelle de la végétation. "
            "Timelapse animation temporal evolution satellite time series."
        ),
        "triggers": [
            "timelapse","time-lapse","animation","évolution temporelle",
            "changement au fil du temps","années","série temporelle",
            "avant après","temporal","historique satellite",
        ],
        "output_action": "add_timelapse",
        "few_shot": [
            {"user": "timelapse NDVI 2018-2024", "tool": "compute_timelapse",
             "params": {"start_date":"2018-01-01","end_date":"2024-12-31",
                        "interval":"year","index":"ndvi"}},
            {"user": "évolution de la forêt depuis 2015", "tool": "compute_timelapse",
             "params": {"start_date":"2015-01-01","end_date":"2024-12-31",
                        "interval":"quarter","index":"ndvi"}},
        ],
    },
    {
        "id":          "gee_change",
        "server":      "gee",
        "tool":        "compute_change_detection",
        "description": (
            "Détecte les changements de végétation entre deux dates (différence NDVI). "
            "Rouge = perte, vert = gain. Déforestation, urbanisation, restauration. "
            "Change detection deforestation urbanization NDVI difference."
        ),
        "triggers": [
            "changement","détection changement","change detection","avant après",
            "déforestation","urbanisation","perte végétation",
            "différence","comparaison dates",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "déforestation 2015 vs 2024", "tool": "compute_change_detection",
             "params": {"date1":"2015-01-01","date2":"2024-01-01"}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # GEE — RADAR SAR
    # ══════════════════════════════════════════════════════════
    {
        "id":          "gee_sar_vv",
        "server":      "gee",
        "tool":        "compute_sar_vv",
        "description": (
            "Image SAR Sentinel-1 polarisation VV. Sensible aux surfaces rugueuses, "
            "bâti, eau et inondations. Pénètre les nuages. "
            "SAR Sentinel-1 VV polarization radar flood buildings water."
        ),
        "triggers": [
            "sar","radar","sentinel-1","vv","polarisation vv",
            "radar satellite","micro-ondes","nuages","inondation radar",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "image radar VV de la zone", "tool": "compute_sar_vv",
             "params": {"start_date":"2024-01-01","end_date":"2024-12-31"}},
        ],
    },
    {
        "id":          "gee_sar_vh",
        "server":      "gee",
        "tool":        "compute_sar_vh",
        "description": (
            "Image SAR Sentinel-1 polarisation VH. Sensible à la végétation "
            "et l'humidité du sol. Complémentaire du VV. "
            "SAR VH polarization vegetation soil moisture radar."
        ),
        "triggers": ["sar vh","polarisation vh","radar végétation","humidité radar"],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "SAR VH végétation", "tool": "compute_sar_vh",
             "params": {"start_date":"2024-01-01","end_date":"2024-12-31"}},
        ],
    },
    {
        "id":          "gee_sar_ratio",
        "server":      "gee",
        "tool":        "compute_sar_vv_vh",
        "description": (
            "Ratio VV/VH Sentinel-1 SAR pour discriminer les types de surface : "
            "eau, sol nu, végétation, bâti. Classification radar. "
            "SAR VV VH ratio surface type classification land cover."
        ),
        "triggers": [
            "ratio vv vh","sar ratio","classification radar",
            "types de surface radar","occupation sol radar",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "classification radar de la zone", "tool": "compute_sar_vv_vh",
             "params": {"start_date":"2024-01-01","end_date":"2024-12-31"}},
        ],
    },
    {
        "id":          "gee_sar_rgb",
        "server":      "gee",
        "tool":        "compute_sar_rgb",
        "description": (
            "Composite SAR fausse couleur RGB : R=VV G=VH B=ratio. "
            "Visualisation intuitive des types de surface par radar. "
            "SAR false color composite RGB Sentinel-1."
        ),
        "triggers": ["sar rgb","sar fausse couleur","composite radar","radar couleur"],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "composite SAR couleur", "tool": "compute_sar_rgb",
             "params": {"start_date":"2024-01-01","end_date":"2024-12-31"}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # GEE — TEMPÉRATURE / CLIMAT
    # ══════════════════════════════════════════════════════════
    {
        "id":          "gee_lst_modis",
        "server":      "gee",
        "tool":        "compute_lst_modis",
        "description": (
            "Température de surface (LST) MODIS jour ET nuit à 1km de résolution. "
            "Ilots de chaleur urbains, stress thermique, analyse climatique. "
            "Land surface temperature MODIS day night urban heat island thermal."
        ),
        "triggers": [
            "lst","température surface","land surface temperature",
            "chaleur","ilot de chaleur","icu","temperature","thermique",
            "modis température","jour nuit température",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "îlots de chaleur urbains de Nantes", "tool": "compute_lst_modis",
             "params": {"start_date":"2024-06-01","end_date":"2024-08-31","mode":"day"}},
            {"user": "température de surface jour et nuit", "tool": "compute_lst_modis",
             "params": {"start_date":"2024-07-01","end_date":"2024-07-31","mode":"both"}},
        ],
    },
    {
        "id":          "gee_lst_landsat",
        "server":      "gee",
        "tool":        "compute_lst_landsat",
        "description": (
            "Température de surface Landsat 8/9 bande thermique à 100m. "
            "Plus précis que MODIS pour les analyses locales. "
            "LST Landsat thermal band 100m surface temperature urban."
        ),
        "triggers": [
            "lst landsat","température landsat","thermique landsat",
            "température 100m","chaleur haute résolution",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "température surface Landsat été 2024", "tool": "compute_lst_landsat",
             "params": {"start_date":"2024-06-01","end_date":"2024-08-31",
                        "collection":"landsat9"}},
        ],
    },
    {
        "id":          "gee_era5_temp",
        "server":      "gee",
        "tool":        "compute_era5_temp",
        "description": (
            "Température de l'air ERA5 Land à 2m d'altitude (°C) à 9km de résolution. "
            "Données climatiques ECMWF. Moyenne, max ou min sur une période. "
            "ERA5 air temperature climate ECMWF 2m temperature mean max min."
        ),
        "triggers": [
            "era5","température air","climat","température atmosphérique",
            "ecmwf","température 2m","météo historique","climatologie",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "température moyenne 2024 en Afrique", "tool": "compute_era5_temp",
             "params": {"start_date":"2024-01-01","end_date":"2024-12-31","stat":"mean"}},
        ],
    },
    {
        "id":          "gee_era5_precip",
        "server":      "gee",
        "tool":        "compute_era5_precip",
        "description": (
            "Précipitations ERA5 Land (mm/jour) à 9km de résolution. "
            "Analyse des pluies, sécheresse, saisons humides. "
            "ERA5 precipitation rainfall mm day climate ECMWF drought."
        ),
        "triggers": [
            "précipitations","pluie","pluviométrie","rainfall","precipitation",
            "sécheresse","saison des pluies","era5 pluie","mm pluie",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "précipitations sahel 2024", "tool": "compute_era5_precip",
             "params": {"start_date":"2024-06-01","end_date":"2024-09-30"}},
        ],
    },
    {
        "id":          "gee_era5_humidity",
        "server":      "gee",
        "tool":        "compute_era5_humidity",
        "description": (
            "Humidité relative ERA5 (%) calculée depuis température et point de rosée. "
            "Analyse du confort thermique, stress hydrique. "
            "Relative humidity ERA5 comfort thermal stress dew point."
        ),
        "triggers": [
            "humidité","humidité relative","hygro","confort thermique",
            "stress hydrique","era5 humidité","humidity",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "humidité relative 2024", "tool": "compute_era5_humidity",
             "params": {"start_date":"2024-01-01","end_date":"2024-12-31"}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # GEE — VÉGÉTATION
    # ══════════════════════════════════════════════════════════
    {
        "id":          "gee_modis_ndvi",
        "server":      "gee",
        "tool":        "compute_modis_ndvi",
        "description": (
            "NDVI MODIS à 250m de résolution, composites 16 jours. "
            "Suivi de la végétation à grande échelle, phénologie. "
            "MODIS NDVI 250m vegetation phenology large scale 16-day composite."
        ),
        "triggers": [
            "modis ndvi","ndvi modis","ndvi 250m","phénologie",
            "grande échelle végétation","suivi végétation",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "MODIS NDVI 2024 Afrique de l'Ouest", "tool": "compute_modis_ndvi",
             "params": {"start_date":"2024-01-01","end_date":"2024-12-31"}},
        ],
    },
    {
        "id":          "gee_worldcover",
        "server":      "gee",
        "tool":        "compute_esa_worldcover",
        "description": (
            "Carte d'occupation du sol ESA WorldCover 2021 à 10m. "
            "11 classes : forêt, agriculture, bâti, eau, sol nu... "
            "ESA WorldCover land cover land use 10m 2021 11 classes."
        ),
        "triggers": [
            "worldcover","occupation du sol","land cover","land use",
            "esa","utilisation des terres","classes d'occupation",
            "forêt agriculture bâti","classification sol",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "occupation du sol ESA", "tool": "compute_esa_worldcover",
             "params": {}},
        ],
    },
    {
        "id":          "gee_forest_watch",
        "server":      "gee",
        "tool":        "compute_forest_watch",
        "description": (
            "Perte de forêt 2001-2023 et couverture arborée 2000 (Hansen GFW). "
            "Analyse de déforestation, conservation forêt, "
            "Global Forest Watch Hansen tree cover loss."
        ),
        "triggers": [
            "déforestation","forest watch","gfw","hansen","perte forêt",
            "couverture arborée","arbre","sylviculture","conservation forêt",
            "deforestation tree loss cover",
        ],
        "output_action": "add_multiple_layers",
        "few_shot": [
            {"user": "déforestation 2001-2023", "tool": "compute_forest_watch",
             "params": {}},
            {"user": "perte de forêt pour 2020", "tool": "compute_forest_watch",
             "params": {"year_loss": 2020}},
        ],
    },
    {
        "id":          "gee_canopy",
        "server":      "gee",
        "tool":        "compute_canopy_height",
        "description": (
            "Hauteur de la canopée forestière en mètres (Meta/WRI 2020). "
            "Analyse de la structure des forêts, biomasse, séquestration carbone. "
            "Canopy height forest structure biomass carbon tree height meters."
        ),
        "triggers": [
            "hauteur canopée","canopy height","hauteur forêt","structure forêt",
            "biomasse","carbone forêt","hauteur arbres",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "hauteur de la canopée", "tool": "compute_canopy_height",
             "params": {}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # GEE — RELIEF
    # ══════════════════════════════════════════════════════════
    {
        "id":          "gee_elevation",
        "server":      "gee",
        "tool":        "compute_elevation",
        "description": (
            "Carte d'élévation SRTM à 30m de résolution. "
            "Altitude, topographie, modèle numérique de terrain MNT. "
            "SRTM elevation altitude DEM digital elevation model terrain 30m."
        ),
        "triggers": [
            "élévation","altitude","srtm","mnt","dem","topographie",
            "relief","modèle numérique terrain","hauteur sol",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "carte d'altitude de la zone", "tool": "compute_elevation",
             "params": {}},
        ],
    },
    {
        "id":          "gee_slope",
        "server":      "gee",
        "tool":        "compute_slope",
        "description": (
            "Carte de pente SRTM en degrés à 30m. "
            "Risques de glissement, aptitude agricole, accessibilité terrain. "
            "SRTM slope gradient degrees terrain landslide agriculture."
        ),
        "triggers": [
            "pente","slope","inclinaison","gradient","glissement terrain",
            "aptitude agricole","terrain accidenté",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "carte de pente du terrain", "tool": "compute_slope",
             "params": {}},
        ],
    },
    {
        "id":          "gee_hillshade",
        "server":      "gee",
        "tool":        "compute_hillshade",
        "description": (
            "Carte d'ombrage (hillshade) SRTM pour visualiser le relief. "
            "Azimuth et zénith configurables. "
            "SRTM hillshade relief shading azimuth zenith visualization."
        ),
        "triggers": [
            "ombrage","hillshade","ombre relief","visualisation relief",
            "relief ombré","carte relief",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "ombrage du relief", "tool": "compute_hillshade",
             "params": {"azimuth": 315, "zenith": 45}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # ORS — ROUTING
    # ══════════════════════════════════════════════════════════
    {
        "id":          "ors_isochrone",
        "server":      "ors",
        "tool":        "compute_isochrone",
        "description": (
            "Calcule une isochrone (zone accessible en X minutes) depuis un point. "
            "Accessibilité piéton, vélo ou voiture. Zone de chalandise, service area. "
            "Isochrone accessibility reachability walking cycling driving minutes."
        ),
        "triggers": [
            "isochrone","accessibilité","accessible","zone accessible",
            "minutes à pied","minutes à vélo","minutes en voiture",
            "périmètre d'accessibilité","zone de chalandise",
            "service area","reachability","temps de trajet",
        ],
        "output_action": "add_isochrone",
        "few_shot": [
            {"user": "isochrone 15 min à pied depuis Dakar", "tool": "compute_isochrone",
             "params": {"center":[-17.44, 14.69],"time_minutes":15,"profile":"foot"}},
            {"user": "zone accessible en 10 min à vélo", "tool": "compute_isochrone",
             "params": {"time_minutes":10,"profile":"bike"}},
        ],
    },
    {
        "id":          "ors_isochrones_multi",
        "server":      "ors",
        "tool":        "compute_isochrones_multi",
        "description": (
            "Isochrones multi-intervalles concentriques (ex: 5, 10, 15 min). "
            "Visualise les zones d'accessibilité par tranche de temps. "
            "Multi-interval isochrones concentric zones accessibility time bands."
        ),
        "triggers": [
            "isochrones multiples","5 10 15 minutes","zones concentriques",
            "multi-isochrone","tranches de temps","niveaux d'accessibilité",
        ],
        "output_action": "add_isochrone",
        "few_shot": [
            {"user": "isochrones 5, 10 et 15 min", "tool": "compute_isochrones_multi",
             "params": {"intervals":[5,10,15],"profile":"foot"}},
        ],
    },
    {
        "id":          "ors_route",
        "server":      "ors",
        "tool":        "compute_route",
        "description": (
            "Calcule un itinéraire entre deux points ou plus avec instructions. "
            "À pied, à vélo ou en voiture. Tour-par-tour en français. "
            "Route itinerary directions walking cycling driving turn-by-turn."
        ),
        "triggers": [
            "itinéraire","route","chemin","directions","comment aller",
            "trajet","aller de à","navigation","plus court chemin",
            "distance","durée trajet",
        ],
        "output_action": "add_route",
        "few_shot": [
            {"user": "itinéraire de la gare à l'aéroport", "tool": "compute_route",
             "params": {"profile":"car"}},
            {"user": "route à vélo entre ces deux points", "tool": "compute_route",
             "params": {"profile":"bike"}},
        ],
    },
    {
        "id":          "ors_matrix",
        "server":      "ors",
        "tool":        "compute_matrix",
        "description": (
            "Matrice de distances et durées entre N points (max 25). "
            "Optimisation logistique, analyse de proximité. "
            "Distance matrix travel time OD matrix logistics proximity."
        ),
        "triggers": [
            "matrice distances","matrix","od matrix","distance entre points",
            "durées entre plusieurs points","logistique",
        ],
        "output_action": "show_matrix",
        "few_shot": [
            {"user": "distances entre ces 5 écoles", "tool": "compute_matrix",
             "params": {"profile":"foot"}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # WORLDBANK
    # ══════════════════════════════════════════════════════════
    {
        "id":          "wb_indicator",
        "server":      "worldbank",
        "tool":        "get_indicator",
        "description": (
            "Affiche un indicateur World Bank sur une carte choroplèthe mondiale. "
            "PIB, population, espérance de vie, CO2, alphabétisation, chômage... "
            "World Bank indicator choropleth GDP population life expectancy CO2."
        ),
        "triggers": [
            "pib","gdp","population mondiale","indicateur mondial",
            "espérance de vie","co2","alphabétisation","chômage",
            "world bank","banque mondiale","données mondiales",
            "carte mondiale","choroplèthe mondial","pays",
            "mortalité","inégalités","gini","développement",
        ],
        "output_action": "add_choropleth",
        "few_shot": [
            {"user": "carte PIB par habitant", "tool": "get_indicator",
             "params": {"indicator":"NY.GDP.PCAP.CD","year":2023}},
            {"user": "population mondiale par pays", "tool": "get_indicator",
             "params": {"indicator":"SP.POP.TOTL"}},
            {"user": "espérance de vie dans le monde", "tool": "get_indicator",
             "params": {"indicator":"SP.DYN.LE00.IN"}},
            {"user": "émissions CO2 par pays", "tool": "get_indicator",
             "params": {"indicator":"EN.ATM.CO2E.PC"}},
        ],
    },
    {
        "id":          "wb_profile",
        "server":      "worldbank",
        "tool":        "get_country_profile",
        "description": (
            "Profil complet d'un pays : 10 indicateurs clés WorldBank. "
            "PIB, population, santé, éducation, environnement. "
            "Country profile World Bank economic social indicators."
        ),
        "triggers": [
            "profil pays","données sénégal","données france","indicateurs pays",
            "fiche pays","statistiques pays","bilan pays",
        ],
        "output_action": "show_country_profile",
        "few_shot": [
            {"user": "profil du Sénégal", "tool": "get_country_profile",
             "params": {"country":"Sénégal"}},
        ],
    },
    {
        "id":          "wb_compare",
        "server":      "worldbank",
        "tool":        "compare_countries",
        "description": (
            "Compare plusieurs pays sur un indicateur WorldBank. "
            "Classement, tableau comparatif. "
            "Compare countries ranking World Bank indicator table."
        ),
        "triggers": [
            "comparer pays","comparaison pays","classement pays",
            "quel pays le plus","meilleur pays","ranking",
        ],
        "output_action": "show_comparison",
        "few_shot": [
            {"user": "comparer PIB Sénégal Mali Guinée", "tool": "compare_countries",
             "params": {"countries":["Sénégal","Mali","Guinée"],
                        "indicator":"NY.GDP.PCAP.CD"}},
        ],
    },
    {
        "id":          "wb_timeseries",
        "server":      "worldbank",
        "tool":        "get_indicator_timeseries",
        "description": (
            "Évolution temporelle d'un indicateur WorldBank de 2000 à aujourd'hui. "
            "Courbe de tendance par pays. "
            "Time series trend evolution indicator 2000 2023 country."
        ),
        "triggers": [
            "évolution","tendance","série temporelle","depuis 2000",
            "graphique historique","courbe PIB","croissance historique",
        ],
        "output_action": "show_timeseries",
        "few_shot": [
            {"user": "évolution PIB Sénégal 2000-2023", "tool": "get_indicator_timeseries",
             "params": {"indicator":"NY.GDP.PCAP.CD","countries":["SEN"],
                        "start_year":2000,"end_year":2023}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # OVERTURE MAPS
    # ══════════════════════════════════════════════════════════
    {
        "id":          "overture_places",
        "server":      "overture",
        "tool":        "query_places",
        "description": (
            "Recherche des points d'intérêt (POI) Overture Maps dans une zone. "
            "Restaurants, pharmacies, hôpitaux, écoles, hôtels, commerces... "
            "Overture Places POI points of interest restaurants pharmacy school."
        ),
        "triggers": [
            "restaurant","pharmacie","hôpital","école","hôtel","commerce",
            "magasin","poi","point d'intérêt","lieux","endroits",
            "établissement","service","équipement","place",
        ],
        "output_action": "add_markers",
        "few_shot": [
            {"user": "restaurants autour de moi", "tool": "query_places",
             "params": {"category":"restaurant","radius_m":1000}},
            {"user": "pharmacies dans le quartier", "tool": "query_places",
             "params": {"category":"pharmacie"}},
        ],
    },
    {
        "id":          "overture_buildings",
        "server":      "overture",
        "tool":        "query_buildings",
        "description": (
            "Bâtiments Overture Maps avec hauteur et nombre d'étages. "
            "Extrusion 3D dans MapLibre. Tissu urbain, densité bâtie. "
            "Buildings height floors 3D extrusion urban fabric density Overture."
        ),
        "triggers": [
            "bâtiments","batiments","buildings","hauteur bâtiments",
            "3d bâtiments","extrusion","tissu urbain","construction",
            "immeuble","maison","résidentiel","commercial",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "bâtiments de plus de 20m", "tool": "query_buildings",
             "params": {"min_height":20}},
            {"user": "carte des bâtiments", "tool": "query_buildings",
             "params": {}},
        ],
    },
    {
        "id":          "overture_roads",
        "server":      "overture",
        "tool":        "query_roads",
        "description": (
            "Réseau routier Overture Maps avec classification. "
            "Autoroutes, routes principales, secondaires, résidentielles. "
            "Roads network highway primary secondary residential Overture."
        ),
        "triggers": [
            "routes","réseau routier","voirie","autoroute","route principale",
            "rue","voie","réseau transport","infrastructure routière",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "réseau routier de la zone", "tool": "query_roads",
             "params": {}},
            {"user": "autoroutes et voies rapides", "tool": "query_roads",
             "params": {"road_class":"motorway"}},
        ],
    },
    {
        "id":          "overture_divisions",
        "server":      "overture",
        "tool":        "query_divisions",
        "description": (
            "Divisions administratives Overture : communes, régions, pays. "
            "Limites administratives, découpages territoriaux. "
            "Administrative divisions boundaries communes regions countries Overture."
        ),
        "triggers": [
            "commune","région","département","pays","division administrative",
            "limites administratives","découpage territorial","frontières",
            "municipality","arrondissement",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "communes de Loire-Atlantique", "tool": "query_divisions",
             "params": {"admin_level":"locality","country":"FR"}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # OSM / OVERPASS
    # ══════════════════════════════════════════════════════════
    {
        "id":          "osm_amenities",
        "server":      "osm",
        "tool":        "get_amenities",
        "description": (
            "Équipements OpenStreetMap dans une zone : hôpitaux, écoles, "
            "pharmacies, restaurants, banques, parkings... "
            "OSM amenities facilities equipment hospital school pharmacy."
        ),
        "triggers": [
            "équipements","osm","openstreetmap","amenity",
            "infrastructure","service public","équipement public",
        ],
        "output_action": "add_markers",
        "few_shot": [
            {"user": "hôpitaux OSM dans la zone", "tool": "get_amenities",
             "params": {"amenity_type":"hôpital"}},
        ],
    },
    {
        "id":          "osm_water",
        "server":      "osm",
        "tool":        "get_water_features",
        "description": (
            "Cours d'eau, lacs, étangs, zones humides OpenStreetMap. "
            "Hydrographie, réseau hydrographique. "
            "OSM water rivers lakes wetlands hydrography streams."
        ),
        "triggers": [
            "cours d'eau","hydrographie","rivière","fleuve","lac","étang",
            "zone humide","marais","canal","réseau hydrographique",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "cours d'eau de la région", "tool": "get_water_features",
             "params": {"water_type":"river"}},
        ],
    },
    {
        "id":          "osm_green",
        "server":      "osm",
        "tool":        "get_green_spaces",
        "description": (
            "Parcs, forêts, jardins, espaces verts OpenStreetMap. "
            "Espaces naturels, végétation urbaine. "
            "OSM parks forests gardens green spaces natural areas urban."
        ),
        "triggers": [
            "parcs","jardins","espaces verts","forêt osm","espace naturel",
            "végétation urbaine","pelouse","prairie osm",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "parcs et espaces verts", "tool": "get_green_spaces",
             "params": {"green_type":"park"}},
        ],
    },
    {
        "id":          "osm_transport",
        "server":      "osm",
        "tool":        "get_public_transport",
        "description": (
            "Transports en commun OSM : arrêts bus, tram, métro, gares. "
            "Réseau de transport public, lignes. "
            "OSM public transport bus tram metro train stops lines network."
        ),
        "triggers": [
            "transport en commun","bus","tram","tramway","métro","gare",
            "arrêt bus","réseau transport","tcsp","transports publics",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "arrêts de tram dans la zone", "tool": "get_public_transport",
             "params": {"mode":"tram"}},
            {"user": "réseau de bus OSM", "tool": "get_public_transport",
             "params": {"mode":"bus"}},
        ],
    },
    {
        "id":          "osm_landuse",
        "server":      "osm",
        "tool":        "get_landuse",
        "description": (
            "Occupation du sol OpenStreetMap : résidentiel, commercial, "
            "industriel, agricole, forestier. "
            "OSM land use landuse residential commercial industrial farmland."
        ),
        "triggers": [
            "landuse","occupation sol osm","zonage","urbanisme","zac",
            "résidentiel","industriel","zone agricole osm",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "zones industrielles OSM", "tool": "get_landuse",
             "params": {"landuse_type":"industrial"}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # POSTGIS — SPATIAL
    # ══════════════════════════════════════════════════════════
    {
        "id":          "postgis_buffer",
        "server":      "postgis",
        "tool":        "spatial_buffer",
        "description": (
            "Calcule une zone tampon (buffer) autour d'une géométrie. "
            "Distance en mètres. Périmètre de protection, zone d'influence. "
            "Buffer zone tampon spatial distance meters protection influence."
        ),
        "triggers": [
            "buffer","zone tampon","tampon","rayon","périmètre",
            "zone autour","distance autour","zone d'influence",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "buffer 500m autour de la couche", "tool": "spatial_buffer",
             "params": {"radius_m":500}},
        ],
    },
    {
        "id":          "postgis_query",
        "server":      "postgis",
        "tool":        "query_table",
        "description": (
            "Requête SELECT sur les tables PostGIS de la base openmapagents. "
            "Données locales : communes, arbres, bâtiments AURAN, canopée. "
            "PostGIS query table local data communes trees buildings canopy."
        ),
        "triggers": [
            "base de données","postgis","données locales","table sql",
            "communes","arbres nantes","bâtiments auran","canopée nantes",
        ],
        "output_action": "add_layer",
        "few_shot": [
            {"user": "communes de Loire-Atlantique depuis la base",
             "tool": "query_table",
             "params": {"table":"communes","filters":{"dept":"44"}}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # NOMINATIM
    # ══════════════════════════════════════════════════════════
    {
        "id":          "nominatim_geocode",
        "server":      "nominatim",
        "tool":        "geocode",
        "description": (
            "Géocode un lieu (nom, adresse, monument) en coordonnées GPS. "
            "Trouve la position d'un endroit nommé. "
            "Geocode place address coordinates GPS location find."
        ),
        "triggers": [
            "où est","localiser","coordonnées","géocoder","adresse",
            "trouver sur la carte","position de","où se trouve",
        ],
        "output_action": "geocode_result",
        "few_shot": [
            {"user": "où est le Château des Ducs de Bretagne",
             "tool": "geocode",
             "params": {"query":"Château des Ducs de Bretagne Nantes"}},
        ],
    },
    {
        "id":          "nominatim_bbox",
        "server":      "nominatim",
        "tool":        "get_bbox_for_place",
        "description": (
            "Retourne la bbox et le zoom recommandé pour une ville, région ou pays. "
            "Centrer la carte sur un lieu. "
            "Bounding box city region country center zoom map."
        ),
        "triggers": [
            "centrer sur","aller à","zoomer sur","afficher","voir",
            "bbox de","limites de la ville","emprise","extent",
        ],
        "output_action": "fly_to_place",
        "few_shot": [
            {"user": "afficher Dakar sur la carte", "tool": "get_bbox_for_place",
             "params": {"place":"Dakar","place_type":"city"}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # STAC
    # ══════════════════════════════════════════════════════════
    {
        "id":          "stac_search",
        "server":      "stac",
        "tool":        "search_catalog",
        "description": (
            "Recherche les scènes satellites disponibles dans le catalog STAC. "
            "Vérifie la disponibilité avant un appel GEE. "
            "STAC catalog search scenes available Sentinel Landsat availability."
        ),
        "triggers": [
            "scènes disponibles","catalog satellite","stac","images disponibles",
            "vérifier disponibilité","combien d'images",
        ],
        "output_action": "show_scenes",
        "few_shot": [
            {"user": "scènes disponibles en 2024 cloud<20%",
             "tool": "search_catalog",
             "params": {"start_date":"2024-01-01","end_date":"2024-12-31",
                        "cloud_cover":20}},
        ],
    },

    # ══════════════════════════════════════════════════════════
    # MAPTILER / ÉLÉVATION
    # ══════════════════════════════════════════════════════════
    {
        "id":          "elev_profile",
        "server":      "maptiler",
        "tool":        "get_elevation_profile",
        "description": (
            "Profil altimétrique le long d'une ligne. "
            "Dénivelés, pente moyenne et max, distance totale. "
            "Elevation profile altitude line slope gradient ascent descent."
        ),
        "triggers": [
            "profil altimétrique","profil altitude","dénivelé",
            "pente d'un chemin","relief d'une route","altitude du tracé",
            "elevation profile","coupe topographique",
        ],
        "output_action": "show_elevation_profile",
        "few_shot": [
            {"user": "profil altimétrique de ce tracé",
             "tool": "get_elevation_profile",
             "params": {"n_points":100}},
        ],
    },
    {
        "id":          "elev_contours",
        "server":      "maptiler",
        "tool":        "get_contours",
        "description": (
            "Courbes de niveau sur une zone. Intervalle configurable (5, 10, 20, 50m). "
            "Topographie, topo carte. "
            "Contour lines topographic map interval meters elevation."
        ),
        "triggers": [
            "courbes de niveau","contours","isohypses","topographique",
            "topo","carte topo","lignes altitude",
        ],
        "output_action": "add_contour_layer",
        "few_shot": [
            {"user": "courbes de niveau toutes les 20m",
             "tool": "get_contours",
             "params": {"interval_m":20}},
        ],
    },
    {
        "id":          "elev_hillshade",
        "server":      "maptiler",
        "tool":        "get_hillshade_url",
        "description": (
            "Tuiles d'ombrage pour visualiser le relief dans MapLibre. "
            "Terrain 3D, hillshade, relief ombré. "
            "Hillshade relief shading MapLibre 3D terrain visualization."
        ),
        "triggers": [
            "relief ombré","3d terrain","terrain maplibre","hillshade url",
            "tuiles relief","fond relief","couche relief",
        ],
        "output_action": "add_hillshade_layer",
        "few_shot": [
            {"user": "ajouter l'ombrage du relief", "tool": "get_hillshade_url",
             "params": {"opacity":0.4}},
        ],
    },
    {
        "id":          "elev_slope_analysis",
        "server":      "maptiler",
        "tool":        "get_slope_analysis",
        "description": (
            "Analyse de pente le long d'une ligne ou sur une grille. "
            "Classification : plat/doux/modéré/fort/très fort. "
            "Slope analysis grid classification flat gentle moderate steep."
        ),
        "triggers": [
            "analyse pente","classification pente","zones de pente",
            "terrain plat","terrain accidenté","pente grid",
        ],
        "output_action": "show_slope_analysis",
        "few_shot": [
            {"user": "analyse de pente sur la zone",
             "tool": "get_slope_analysis",
             "params": {"classify":True}},
        ],
    },
]

# ── Index par server ──────────────────────────────────────────
REGISTRY_BY_SERVER: dict[str, list] = {}
for _entry in TOOL_REGISTRY:
    _s = _entry["server"]
    REGISTRY_BY_SERVER.setdefault(_s, []).append(_entry)

# ── Index par tool ────────────────────────────────────────────
REGISTRY_BY_TOOL: dict[str, dict] = {
    e["tool"]: e for e in TOOL_REGISTRY
}

# ── Tous les triggers → tool_id (pour boost exact match) ─────
TRIGGER_INDEX: dict[str, str] = {}
for _entry in TOOL_REGISTRY:
    for _trigger in _entry["triggers"]:
        TRIGGER_INDEX[_trigger.lower()] = _entry["id"]


def get_tools_for_server(server: str) -> list[dict]:
    return REGISTRY_BY_SERVER.get(server, [])


def get_tool(tool_name: str) -> dict | None:
    return REGISTRY_BY_TOOL.get(tool_name)


def get_few_shots(tool_name: str) -> list[dict]:
    entry = REGISTRY_BY_TOOL.get(tool_name, {})
    return entry.get("few_shot", [])


def find_by_trigger(query: str) -> list[str]:
    """Retourne les tool_ids dont un trigger est dans la query."""
    q = query.lower()
    return list({
        tool_id for trigger, tool_id in TRIGGER_INDEX.items()
        if trigger in q
    })
