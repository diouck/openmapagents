/**
 * datasetMeta.js — Fiches descriptives des indicateurs et de leurs sources.
 *
 * Deux niveaux, volontairement séparés :
 *   • SOURCE_META    : la DONNÉE (capteur, résolution, couverture, producteur).
 *     Un même jeu sert plusieurs indicateurs — on ne le décrit qu'une fois.
 *   • INDICATOR_DOC  : la MESURE (ce qu'elle vaut, comment la lire, ses limites).
 *
 * Les liens de catalogue Earth Engine sont DÉRIVÉS de l'identifiant d'asset
 * (`geeCatalog`), jamais saisis à la main : c'est ce qui garantit qu'ils
 * pointent au bon endroit. La page de catalogue porte aussi la citation
 * scientifique et la licence officielles — c'est la référence à opposer en cas
 * de doute sur les valeurs indicatives ci-dessous.
 */

/** Page de catalogue Earth Engine d'un asset (`A/B/C` → `A_B_C`). */
export function geeCatalog(asset) {
  if (!asset) return null;
  if (asset.startsWith("projects/")) return null;   // asset communautaire : pas de fiche catalogue
  return `https://developers.google.com/earth-engine/datasets/catalog/${asset.replace(/\//g, "_")}`;
}

// ── Sources de données ───────────────────────────────────────────────────────
// asset : identifiant Earth Engine (extrait du catalogue backend, cf. DATASETS)
export const SOURCE_META = {
  sentinel2: {
    label: "Sentinel-2 MSI — réflectance de surface",
    provider: "ESA / Copernicus",
    asset: "COPERNICUS/S2_SR_HARMONIZED",
    res: "10–20 m", revisit: "5 jours (2 satellites)", coverage: "depuis 2017",
    note: "Produit L2A corrigé des effets atmosphériques. Le masque nuages s'appuie sur la bande SCL. La série « harmonisée » corrige le décalage de calibration introduit fin 2021.",
  },
  landsat: {
    label: "Landsat 4/5/7/8/9 — série fusionnée",
    provider: "USGS / NASA",
    asset: "LANDSAT/LC09/C02/T1_L2",
    res: "30 m", revisit: "16 jours par satellite", coverage: "depuis 1984",
    note: "Les missions sont enchaînées automatiquement selon la date et les bandes renommées dans un jeu commun (RED, NIR, SWIR1…), ce qui permet des séries de plus de quarante ans. Landsat 7 souffre d'un défaut de balayage (bandes manquantes) après 2003.",
  },
  landsat8: { label: "Landsat 8 OLI/TIRS", provider: "USGS / NASA", asset: "LANDSAT/LC08/C02/T1_L2",
    res: "30 m (100 m thermique)", revisit: "16 jours", coverage: "depuis 2013" },
  landsat9: { label: "Landsat 9 OLI-2/TIRS-2", provider: "USGS / NASA", asset: "LANDSAT/LC09/C02/T1_L2",
    res: "30 m (100 m thermique)", revisit: "16 jours", coverage: "depuis 2021" },
  sentinel1: {
    label: "Sentinel-1 SAR (GRD)", provider: "ESA / Copernicus", asset: "COPERNICUS/S1_GRD",
    res: "10 m", revisit: "6–12 jours", coverage: "depuis 2014",
    note: "Radar : traverse les nuages et fonctionne de nuit. Les valeurs sont des coefficients de rétrodiffusion en décibels, pas des réflectances — un sol lisse ou une eau calme apparaissent très sombres.",
  },
  modis_ndvi: { label: "MODIS Terra — indices de végétation (MOD13A1)", provider: "NASA LP DAAC",
    asset: "MODIS/061/MOD13A1", res: "500 m", revisit: "composite 16 jours", coverage: "depuis 2000" },
  modis_rgb: { label: "MODIS — réflectance ajustée BRDF (MCD43A4)", provider: "NASA LP DAAC",
    asset: "MODIS/061/MCD43A4", res: "500 m", revisit: "quotidien (fenêtre BRDF 16 jours)", coverage: "depuis 2000",
    note: "Réflectance de surface nadir corrigée des effets d'angle (BRDF) en combinant Terra et Aqua : vraies couleurs homogènes à l'échelle continentale/mondiale, sans les rayures d'angle des produits journaliers bruts. Pas de masque nuages par défaut — élargir la période pour une mosaïque plus propre." },
  modis_lst: { label: "MODIS Terra — température de surface (MOD11A1)", provider: "NASA LP DAAC",
    asset: "MODIS/061/MOD11A1", res: "1 km", revisit: "quotidien", coverage: "depuis 2000",
    units: "kelvin dans la donnée source, converti en °C à l'affichage" },
  modis_et: { label: "MODIS — évapotranspiration (MOD16A2)", provider: "NASA LP DAAC",
    asset: "MODIS/061/MOD16A2", res: "500 m", revisit: "composite 8 jours", coverage: "depuis 2001",
    units: "kg/m²/8 j, équivalent à des mm sur la période" },
  geos_cf: { label: "NASA GEOS-CF — prévision de composition atmosphérique", provider: "NASA GMAO",
    asset: "NASA/GEOS-CF/v1/fcst/tavg1hr", res: "~27 km (0,25°)", revisit: "horaire (prévision 5 j)", coverage: "2022 → 2026",
    units: "PM2.5 et espèces carbonées en µg/m³" },
  cams: { label: "CAMS — surveillance de l'atmosphère (Copernicus/ECMWF)", provider: "ECMWF / Copernicus",
    asset: "ECMWF/CAMS/NRT", res: "~44 km (0,4°)", revisit: "quotidien (2 prévisions 5 j/jour)", coverage: "depuis 2016, temps réel",
    units: "PM2.5 en µg/m³ ; AOD sans dimension" },
  modis_gpp: { label: "MODIS — productivité primaire brute (MOD17A2H)", provider: "NASA LP DAAC",
    asset: "MODIS/061/MOD17A2H", res: "500 m", revisit: "composite 8 jours", coverage: "depuis 2000",
    units: "kg C/m²/8 j" },
  modis_lai: { label: "MODIS — LAI et FAPAR (MCD15A3H)", provider: "NASA LP DAAC",
    asset: "MODIS/061/MCD15A3H", res: "500 m", revisit: "composite 4 jours", coverage: "depuis 2002",
    units: "LAI en m²/m² ; FAPAR sans unité (0–1)" },
  modis_snow: { label: "MODIS — couverture neigeuse (MOD10A1)", provider: "NASA NSIDC",
    asset: "MODIS/061/MOD10A1", res: "500 m", revisit: "quotidien", coverage: "depuis 2000",
    units: "NDSI Snow Cover, 0–100 %" },
  burned: { label: "MODIS — surfaces brûlées (MCD64A1)", provider: "NASA LP DAAC",
    asset: "MODIS/061/MCD64A1", res: "500 m", revisit: "mensuel", coverage: "depuis 2000",
    note: "La bande utilisée est le jour de l'année de détection du brûlage, pas une intensité." },
  firms: { label: "FIRMS — foyers actifs", provider: "NASA FIRMS", asset: "FIRMS",
    res: "1 km", revisit: "quasi temps réel", coverage: "depuis 2000",
    note: "Détecte la chaleur au moment du passage du satellite : un feu éteint entre deux passages n'apparaît pas." },
  worldcover: { label: "ESA WorldCover — occupation du sol", provider: "ESA", asset: "ESA/WorldCover/v200",
    res: "10 m", revisit: "millésime annuel", coverage: "2021 (v200)",
    note: "11 classes discrètes. Une classification n'a pas de valeurs intermédiaires : ne pas y appliquer de moyenne." },
  dynamicworld: { label: "Dynamic World — occupation du sol quasi temps réel", provider: "Google / WRI",
    asset: "GOOGLE/DYNAMICWORLD/V1", res: "10 m", revisit: "~5 jours", coverage: "depuis 2015",
    note: "Produit par apprentissage profond à partir de Sentinel-2 : une classe par image, agrégée ici par classe majoritaire sur la période." },
  hansen: { label: "Global Forest Change — couvert et pertes forestières", provider: "UMD / GLAD",
    asset: "UMD/hansen/global_forest_change_2023_v1_11", res: "30 m", revisit: "millésime annuel",
    coverage: "2000–2023",
    note: "« Perte » signifie disparition du couvert arboré, toutes causes confondues — coupe, incendie, tempête, exploitation. Ce n'est pas un synonyme de déforestation." },
  canopy_height: { label: "Hauteur de canopée mondiale à 1 m", provider: "Meta / World Resources Institute",
    asset: "projects/meta-forest-monitoring-okw37/assets/CanopyHeight", res: "1 m", revisit: "statique",
    coverage: "imagerie ~2009–2020",
    note: "Asset communautaire (pas de fiche au catalogue public). Hauteurs estimées par apprentissage sur imagerie très haute résolution ; les valeurs sous 1 m sont masquées." },
  gedi_agb: { label: "GEDI L4B — biomasse aérienne", provider: "NASA / LARSE", asset: "LARSE/GEDI/GEDI04_B_002",
    res: "1 km", revisit: "statique", coverage: "2019–2023", units: "Mg/ha (tonnes de matière sèche par hectare)",
    note: "Agrégation à 1 km de mesures lidar par empreintes ; couverture limitée à ±52° de latitude." },
  jrc_water: { label: "JRC Global Surface Water", provider: "Commission européenne — JRC",
    asset: "JRC/GSW1_4/GlobalSurfaceWater", res: "30 m", revisit: "statique", coverage: "1984–2021",
    units: "occurrence en % du temps où le pixel est en eau",
    note: "Construit sur l'archive Landsat complète : une occurrence de 50 % signale un régime saisonnier, pas une incertitude." },
  smap: { label: "SMAP L4 — humidité du sol", provider: "NASA", asset: "NASA/SMAP/SPL4SMGP/007",
    res: "~10 km", revisit: "3 heures", coverage: "depuis 2015", units: "m³/m³ (fraction volumique)" },
  chirps: { label: "CHIRPS — précipitations quotidiennes", provider: "UC Santa Barbara / USGS",
    asset: "UCSB-CHG/CHIRPS/DAILY", res: "~5 km", revisit: "quotidien", coverage: "depuis 1981",
    units: "mm — cumulés sur la période demandée",
    note: "Croise imagerie infrarouge et stations au sol. Conçu pour le suivi des sécheresses, particulièrement fiable sous les tropiques." },
  gpm: { label: "GPM IMERG — précipitations mensuelles", provider: "NASA", asset: "NASA/GPM_L3/IMERG_MONTHLY_V07",
    res: "~11 km", revisit: "mensuel", coverage: "depuis 2000", units: "mm/h (moyenne mensuelle)" },
  era5: { label: "ERA5-Land — réanalyse climatique mensuelle", provider: "ECMWF / Copernicus",
    asset: "ECMWF/ERA5_LAND/MONTHLY_AGGR", res: "~11 km", revisit: "mensuel", coverage: "depuis 1950",
    note: "Réanalyse : un modèle contraint par les observations, pas une mesure directe. Homogène dans le temps, donc adapté aux comparaisons pluri-décennales." },
  era5_solar: { label: "ERA5-Land — rayonnement solaire", provider: "ECMWF / Copernicus",
    asset: "ECMWF/ERA5_LAND/MONTHLY_AGGR", res: "~11 km", revisit: "mensuel", coverage: "depuis 1950",
    units: "J/m² cumulés" },
  era5_wind: { label: "ERA5-Land — vent à 10 m", provider: "ECMWF / Copernicus",
    asset: "ECMWF/ERA5_LAND/MONTHLY_AGGR", res: "~11 km", revisit: "mensuel", coverage: "depuis 1950",
    units: "m/s (norme des composantes u et v)" },
  sentinel5p: { label: "Sentinel-5P TROPOMI — composition atmosphérique", provider: "ESA / Copernicus",
    asset: "COPERNICUS/S5P/OFFL/L3_NO2", res: "~7 km", revisit: "quotidien", coverage: "depuis 2018",
    note: "Colonne intégrée sur toute l'épaisseur de l'atmosphère, non une concentration au sol. Une collection distincte par polluant." },
  viirs: { label: "VIIRS — lumières nocturnes", provider: "NOAA", asset: "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG",
    res: "~500 m", revisit: "mensuel", coverage: "depuis 2014", units: "nW/cm²/sr",
    note: "Version corrigée des nuages et de la lumière lunaire ; les feux et les navires subsistent comme artefacts." },
  ghsl_pop: { label: "GHSL — population maillée", provider: "Commission européenne — JRC",
    asset: "JRC/GHSL/P2023A/GHS_POP", res: "100 m", revisit: "époques tous les 5 ans", coverage: "1975–2030",
    units: "habitants par cellule", note: "Les époques 2025 et 2030 sont des projections, non des observations." },
  ghsl_built: { label: "GHSL — surface bâtie", provider: "Commission européenne — JRC",
    asset: "JRC/GHSL/P2023A/GHS_BUILT_S", res: "100 m", revisit: "époques tous les 5 ans", coverage: "1975–2030",
    units: "m² bâtis par cellule" },
  ghsl_smod: { label: "GHSL — degré d'urbanisation", provider: "Commission européenne — JRC",
    asset: "JRC/GHSL/P2023A/GHS_SMOD_V2-0", res: "1 km", revisit: "époques tous les 5 ans", coverage: "1975–2030",
    note: "Classification officielle de l'UE et de l'ONU (centre urbain, périurbain, rural…), base statistique du « degree of urbanisation »." },
  gpw_pop: { label: "GPWv4.11 — densité de population", provider: "NASA SEDAC / CIESIN",
    asset: "CIESIN/GPWv411/GPW_Population_Density", res: "~1 km", revisit: "tous les 5 ans", coverage: "2000–2020",
    units: "habitants/km²",
    note: "Redistribue les recensements sans modèle d'occupation du sol : plus lisse que GHSL, et donc moins précis en ville." },
  srtm: { label: "SRTM — modèle numérique de terrain", provider: "NASA / USGS", asset: "USGS/SRTMGL1_003",
    res: "30 m", revisit: "statique", coverage: "acquisition février 2000", units: "mètres au-dessus du géoïde",
    note: "Couverture limitée à ±60° de latitude. Modèle de SURFACE : il inclut la canopée et le bâti." },
  copdem: { label: "Copernicus DEM GLO-30", provider: "ESA / Airbus", asset: "COPERNICUS/DEM/GLO30",
    res: "30 m", revisit: "statique", coverage: "acquisitions 2011–2015", units: "mètres",
    note: "Issu du radar TanDEM-X, globalement plus récent et plus propre que SRTM, y compris aux hautes latitudes." },
  etopo: { label: "ETOPO1 — relief et bathymétrie", provider: "NOAA NGDC", asset: "NOAA/NGDC/ETOPO1",
    res: "~1,8 km", revisit: "statique", coverage: "global", units: "mètres (négatifs sous le niveau de la mer)" },
  soil_soc: { label: "OpenLandMap — carbone organique du sol", provider: "OpenGeoHub / OpenLandMap",
    asset: "OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02", res: "250 m", revisit: "statique",
    units: "g/kg", note: "Prédiction par apprentissage à partir de profils de sol ; plusieurs profondeurs disponibles dans la donnée source." },
  soil_ph: { label: "OpenLandMap — pH du sol (H₂O)", provider: "OpenGeoHub / OpenLandMap",
    asset: "OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02", res: "250 m", revisit: "statique",
    units: "pH × 10 dans la donnée source" },
  soil_clay: { label: "OpenLandMap — teneur en argile", provider: "OpenGeoHub / OpenLandMap",
    asset: "OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02", res: "250 m", revisit: "statique",
    units: "% massique" },
  // ── Océanographie ──
  modis_ocean: { label: "MODIS-Aqua — couleur de l'océan (L3SMI)", provider: "NASA Ocean Biology (OB.DAAC)",
    asset: "NASA/OCEANDATA/MODIS-Aqua/L3SMI", res: "~4 km", revisit: "quotidien (composites)", coverage: "depuis 2002",
    units: "SST en °C ; chlorophylle et POC en mg/m³",
    note: "Produit L3 « mapped » : les pixels nuageux ou sous glace sont déjà absents, il n'y a donc pas de masque nuages à appliquer. Ne couvre que les surfaces en eau libre." },
  viirs_ocean: { label: "VIIRS-SNPP — couleur de l'océan (L3SMI)", provider: "NASA Ocean Biology (OB.DAAC)",
    asset: "NASA/OCEANDATA/VIIRS-SNPP/L3SMI", res: "~4 km", revisit: "quotidien (composites)", coverage: "depuis 2012",
    note: "Continuité de la série MODIS pour la couleur de l'eau ; capteur plus récent, calibration indépendante." },
  oisst: { label: "NOAA OISST v2.1 — SST optimale interpolée", provider: "NOAA NCEI",
    asset: "NOAA/CDR/OISST/V2_1", res: "~27 km (0,25°)", revisit: "quotidien", coverage: "depuis 1981",
    units: "SST et anomalie en °C ; glace en % de concentration",
    note: "Analyse combinant satellites et bouées, sans trous : idéale pour les séries longues et la détection de canicules marines. La résolution grossière lisse le côtier." },
  hycom_vel: { label: "HYCOM — vitesse des courants marins", provider: "US Navy / NOPP (HYCOM consortium)",
    asset: "HYCOM/sea_water_velocity", res: "~9 km (0,08°)", revisit: "quotidien", coverage: "depuis 1992",
    units: "m/s (composantes u/v mises à l'échelle)",
    note: "Sortie de MODÈLE océanique assimilant des observations, pas une mesure directe. Disponible à plusieurs profondeurs ; on affiche la surface (0 m)." },
  hycom_sal: { label: "HYCOM — température et salinité", provider: "US Navy / NOPP (HYCOM consortium)",
    asset: "HYCOM/sea_temp_salinity", res: "~9 km (0,08°)", revisit: "quotidien", coverage: "depuis 1992",
    units: "salinité en PSU (facteur 0,001, décalage +20)",
    note: "Modèle océanique assimilé. Remplace ici les capteurs SMOS/Aquarius, absents du catalogue GEE. Salinité de surface (0 m)." },
  era5_ocean_wind: { label: "ERA5 — réanalyse atmosphérique (horaire)", provider: "ECMWF / Copernicus C3S",
    asset: "ECMWF/ERA5/HOURLY", res: "~31 km (0,25°)", revisit: "horaire", coverage: "depuis 1940",
    units: "m/s (module du vent à 10 m)",
    note: "ERA5 « complet » (et non ERA5-Land, vide sur mer) : couvre les océans. Le vent affiché est la moyenne de ‖(u,v)‖ à 10 m sur la période." },
  coral: { label: "Allen Coral Atlas — habitats récifaux", provider: "Allen Coral Atlas / Vulcan",
    asset: "ACA/reef_habitat/v2_0", res: "~5 m", revisit: "statique (2018–2020)", coverage: "récifs tropicaux mondiaux",
    note: "Cartographie mondiale des récifs dérivée d'images Planet/Sentinel-2. On affiche l'emprise récifale ; classes géomorphologiques et benthiques disponibles dans la donnée source." },
  mangrove: { label: "Global Mangrove Forests 2000", provider: "Giri et al. / USGS-NASA",
    asset: "LANDSAT/MANGROVE_FORESTS", res: "30 m", revisit: "statique (2000)", coverage: "ceinture intertropicale",
    units: "présence (1 = mangrove)",
    note: "État de référence de l'an 2000 dérivé de Landsat : ne reflète ni les pertes ni les gains postérieurs." },
  // ── GEDI — LiDAR spatial (NASA) ──
  gedi_l2a: { label: "GEDI L2A — hauteur et élévation (LiDAR)", provider: "NASA / University of Maryland",
    asset: "LARSE/GEDI/GEDI02_A_002_MONTHLY", res: "25 m (empreintes)", revisit: "composite mensuel",
    coverage: "depuis avril 2019, latitudes ±51,6°", units: "mètres (métriques RH, élévation)",
    note: "LiDAR à pleine onde depuis l'ISS. Échantillonnage par EMPREINTES le long des traces, grillé au mois — la couverture n'est pas continue et se densifie sur de longues périodes. Absent au-delà de ±51,6° de latitude." },
  gedi_l2b: { label: "GEDI L2B — structure de la canopée (LiDAR)", provider: "NASA / University of Maryland",
    asset: "LARSE/GEDI/GEDI02_B_002_MONTHLY", res: "25 m (empreintes)", revisit: "composite mensuel",
    coverage: "depuis avril 2019, latitudes ±51,6°", units: "couverture 0–1, PAI m²/m², FHD sans unité",
    note: "Métriques de structure verticale du couvert dérivées de la forme d'onde LiDAR. Même échantillonnage par empreintes que L2A." },
  gedi_l4a: { label: "GEDI L4A — biomasse aérienne à l'empreinte", provider: "NASA / ORNL DAAC",
    asset: "LARSE/GEDI/GEDI04_A_002_MONTHLY", res: "25 m (empreintes)", revisit: "composite mensuel",
    coverage: "depuis avril 2019, latitudes ±51,6°", units: "Mg/ha (densité de biomasse aérienne)",
    note: "Biomasse par modèle à partir des métriques de hauteur, à la résolution de l'empreinte — plus fine que le produit grillé L4B à 1 km, mais épars." },
};

// ── Fiches par indicateur ────────────────────────────────────────────────────
// abstract : à quoi ça sert ‖ formula : calcul exact tel qu'implémenté
// reading  : comment lire les valeurs ‖ caveat : le piège classique
const D = (abstract, formula, reading, caveat) => ({ abstract, formula, reading, caveat });

export const INDICATOR_DOC = {
  RGBIMG: D(
    "Composition en vraies couleurs : les bandes rouge, verte et bleue du capteur sont affectées aux canaux correspondants de l'écran. On voit la scène telle que l'œil la percevrait depuis l'orbite. C'est l'image de constat par excellence — pour vérifier ce qui est réellement au sol avant d'interpréter un indice calculé.",
    "Rouge → R, Vert → V, Bleu → B",
    "Aucune valeur à lire : c'est une image d'observation, pas une mesure. Les nuages sont blancs, l'eau sombre, la végétation vert foncé.",
    "Le masque nuages est désactivé par défaut sur ce composite : il effacerait fumées et panaches, c'est-à-dire souvent l'événement lui-même. Les scènes couvertes sont donc conservées."
  ),
  IRCIMG: D(
    "Composition en infrarouge couleur, historiquement celle des films infrarouges de l'aviation. Le proche infrarouge est affiché en rouge, le rouge en vert, le vert en bleu. La végétation, très réfléchissante dans le proche infrarouge, apparaît en rouge vif — d'autant plus intense qu'elle est vigoureuse. C'est le composite de référence pour repérer d'un coup d'œil l'état d'un couvert et la limite eau/terre.",
    "PIR → R, Rouge → V, Vert → B",
    "Rouge vif : végétation dense et active. Rose pâle ou beige : couvert clairsemé, sol nu. Bleu-noir : eau (le PIR y est totalement absorbé). Cyan : zones urbaines.",
    "Ne pas confondre avec une mesure : deux images acquises à des dates ou des angles solaires différents ne sont pas comparables quantitativement. Pour cela, utiliser le NDVI."
  ),
  SWIRIMG: D(
    "Composition en moyen infrarouge, l'outil de terrain des feux de forêt. Le moyen infrarouge traverse largement la fumée, là où le visible ne montre qu'un panache opaque, et sature sur les surfaces très chaudes. On distingue donc simultanément le front de flamme actif, la surface déjà brûlée et la végétation encore intacte — sur une seule image, sans traitement.",
    "MIR₂ → R, MIR₁ → V, PIR → B  [S2 : B12/B11/B8A]",
    "Orange à rouge saturé : combustion active. Brun-rouge sombre : brûlis récent. Vert franc : végétation saine. Bleu : eau et sols humides. Le panache de fumée devient translucide.",
    "La détection d'un front actif dépend de l'heure de passage du satellite : un feu éteint entre deux acquisitions est invisible. Pour un suivi continu, croiser avec les foyers actifs FIRMS."
  ),
  NDVI: D(
    "L'indice de végétation par différence normalisée est la mesure de télédétection la plus employée pour suivre la végétation. Il exploite le contraste entre la forte réflexion du proche infrarouge par les feuilles saines et l'absorption du rouge par la chlorophylle. Il sert à cartographier le couvert végétal, suivre les cycles saisonniers, détecter le stress hydrique et estimer les rendements agricoles.",
    "(PIR − Rouge) / (PIR + Rouge)",
    "De −1 à +1. Eau et neige sous 0 ; sols nus 0,1–0,2 ; végétation clairsemée 0,2–0,4 ; cultures et prairies actives 0,4–0,7 ; forêt dense au-delà de 0,7.",
    "L'indice sature en forte biomasse : au-dessus de 0,8 il cesse de distinguer les couverts denses — préférer l'EVI. Il est aussi sensible à la couleur du sol quand le couvert est partiel."
  ),
  EVI: D(
    "L'indice de végétation amélioré corrige deux faiblesses du NDVI : la saturation en biomasse élevée et la perturbation par les aérosols atmosphériques, grâce à un terme correctif utilisant le bleu. Il est privilégié en forêt tropicale et pour comparer des couverts très denses.",
    "2,5 × (PIR − Rouge) / (PIR + 6×Rouge − 7,5×Bleu + 1)",
    "De −1 à +1, mais reste discriminant là où le NDVI plafonne.",
    "Demande une bande bleue fiable : plus bruité que le NDVI sur des images mal corrigées de l'atmosphère."
  ),
  SAVI: D(
    "L'indice ajusté pour le sol introduit un facteur de correction L qui atténue l'influence du substrat quand la végétation ne couvre pas complètement le sol. Il est adapté aux milieux arides, aux cultures en début de cycle et aux zones de reprise après perturbation.",
    "(PIR − Rouge) × (1 + L) / (PIR + Rouge + L), avec L = 0,5",
    "Même plage que le NDVI, avec des valeurs plus basses à couvert égal.",
    "Le facteur L = 0,5 vise un couvert intermédiaire ; sur couvert très dense ou sol totalement nu, il n'est plus optimal."
  ),
  GNDVI: D(
    "Variante du NDVI utilisant le vert plutôt que le rouge. Le vert est davantage lié à la concentration en chlorophylle qu'à la simple présence de biomasse, ce qui rend l'indice plus sensible à l'état nutritionnel — notamment azoté — des cultures.",
    "(PIR − Vert) / (PIR + Vert)",
    "De −1 à +1 ; sature plus tard que le NDVI sur couvert dense.",
    "Plus sensible aux effets atmosphériques résiduels que le NDVI."
  ),
  NDRE: D(
    "L'indice red-edge exploite la bande de transition entre rouge et proche infrarouge, où la réflectance varie fortement avec la teneur en chlorophylle. Il détecte un stress avant qu'il ne soit visible sur le NDVI, ce qui en fait un outil d'agriculture de précision pour le pilotage de la fertilisation.",
    "(PIR − Red-edge) / (PIR + Red-edge)  [bandes B8 et B5]",
    "Typiquement 0,2–0,6 sur culture active ; c'est l'évolution relative dans la parcelle qui informe, plus que la valeur absolue.",
    "Nécessite une bande red-edge : disponible sur Sentinel-2 uniquement, pas sur Landsat."
  ),
  CANOPY: D(
    "Modèle mondial de hauteur de canopée produit par apprentissage profond sur imagerie submétrique, calibré sur des mesures lidar. Il permet de caractériser la structure forestière, repérer les arbres hors forêt et les haies, et sert d'entrée aux estimations de biomasse.",
    null,
    "Hauteur en mètres ; les valeurs inférieures à 1 m sont masquées pour éliminer le bruit sur sol nu.",
    "Modèle statistique et non mesure directe : les hauteurs extrêmes sont sous-estimées et le résultat est daté de l'imagerie sous-jacente, pas de l'année en cours."
  ),
  FOREST: D(
    "Global Forest Change suit annuellement le couvert arboré mondial depuis 2000 à partir de l'archive Landsat. Trois couches sont proposées : le couvert de référence en 2000, les pertes et les gains constatés depuis. C'est la source de référence des bilans de déforestation.",
    null,
    "Couvert 2000 en % ; pertes et gains en couches binaires (pixel touché ou non).",
    "La « perte de couvert » recouvre toutes les causes — coupe rase, incendie, tempête, récolte de plantation. L'assimiler à de la déforestation surestime fortement le phénomène."
  ),
  BIOMASS: D(
    "Biomasse aérienne dérivée du lidar spatial GEDI, agrégée sur une maille kilométrique. Elle quantifie la matière végétale sur pied et, par conversion, le carbone stocké — donnée centrale des inventaires carbone et des projets de crédits forestiers.",
    null,
    "En mégagrammes (tonnes) de matière sèche par hectare. Le carbone représente environ la moitié de cette masse.",
    "GEDI n'observe qu'entre 52° de latitude nord et sud : aucune donnée aux hautes latitudes, boréales comprises."
  ),
  GPP: D(
    "La productivité primaire brute mesure le carbone fixé par la photosynthèse par unité de surface et de temps. Elle traduit l'activité réelle de l'écosystème, là où le NDVI ne renseigne que sur la quantité de feuillage présent.",
    null,
    "En kilogrammes de carbone par mètre carré et par période de 8 jours ; cumuler pour obtenir un bilan annuel.",
    "Produit modélisé à partir d'un modèle climatique et d'une efficience d'utilisation de la lumière propre à chaque type de couvert : l'incertitude est plus forte sur les milieux mixtes."
  ),
  LAIFAPAR: D(
    "Deux variables biophysiques complémentaires. L'indice de surface foliaire (LAI) est la surface de feuilles par unité de surface au sol. La fraction de rayonnement photosynthétiquement actif absorbé (FAPAR) est la part du rayonnement utile capté par le couvert : c'est la variable d'entrée des modèles de productivité et un indicateur climatique essentiel reconnu par le GCOS.",
    null,
    "LAI en m²/m², de 0 à environ 7 (au-delà en forêt tropicale). FAPAR sans unité, de 0 à 1.",
    "Les deux saturent sur couvert très dense et dépendent d'hypothèses sur l'architecture du couvert."
  ),
  NDWI: D(
    "Indice d'eau de McFeeters : il maximise la réponse des surfaces en eau libre et minimise celle du sol et de la végétation. Il sert à délimiter lacs, rivières et étendues inondées.",
    "(Vert − PIR) / (Vert + PIR)",
    "Valeurs positives sur l'eau, négatives sur le sol et la végétation ; le seuil 0 constitue un premier découpage raisonnable.",
    "Confond l'eau et le bâti, qui ressort également positif : en milieu urbain, préférer le MNDWI."
  ),
  MNDWI: D(
    "Indice d'eau modifié : le moyen infrarouge remplace le proche infrarouge, ce qui sépare nettement l'eau des surfaces artificialisées. C'est l'indice à retenir pour cartographier l'eau en contexte urbain ou périurbain.",
    "(Vert − MIR₁) / (Vert + MIR₁)",
    "Positif sur l'eau ; le contraste avec le bâti est bien plus net qu'avec le NDWI.",
    "Formule identique à celle du NDSI (neige) : sur une image hivernale ou de montagne, neige et eau se ressemblent."
  ),
  NDMI: D(
    "L'indice d'humidité renseigne sur la teneur en eau des feuilles. Il chute avant que la végétation ne jaunisse, ce qui en fait un signal précoce de stress hydrique et un indicateur du risque de départ de feu.",
    "(PIR − MIR₁) / (PIR + MIR₁)",
    "De −1 à +1 : négatif en végétation stressée ou sol sec, positif en couvert bien alimenté en eau.",
    "L'interprétation dépend du type de couvert : comparer une même parcelle dans le temps plutôt que deux milieux différents."
  ),
  NDCI: D(
    "Indice de chlorophylle des eaux de surface, conçu pour suivre l'eutrophisation et les proliférations d'algues en lacs, retenues et estuaires.",
    "(Red-edge − Rouge) / (Red-edge + Rouge)  [bandes B5 et B4]",
    "Croît avec la concentration en chlorophylle a ; à interpréter en tendance, l'étalonnage absolu demandant des mesures in situ.",
    "Valable uniquement sur l'eau : masquer les terres au préalable, sans quoi les valeurs terrestres sont ininterprétables."
  ),
  JRC_WATER: D(
    "Le Global Surface Water du JRC retrace trente-cinq années de dynamique des eaux de surface à partir de l'intégralité de l'archive Landsat. Il distingue les eaux permanentes des eaux saisonnières et documente apparitions et disparitions.",
    null,
    "Occurrence en pourcentage du temps où le pixel a été observé en eau : 100 % pour un plan d'eau permanent, 20–60 % pour une zone d'inondation saisonnière.",
    "Une valeur intermédiaire traduit un régime saisonnier réel, pas une incertitude de mesure."
  ),
  SMAP: D(
    "Humidité du sol issue du radiomètre en bande L de SMAP, assimilée dans un modèle de surface. Elle alimente le suivi agronomique, la prévision hydrologique et l'anticipation des sécheresses.",
    null,
    "Fraction volumique en m³/m³ : environ 0,05 en sol très sec, 0,40 proche de la saturation.",
    "Résolution de l'ordre de 10 km : la valeur est une moyenne de paysage, inadaptée à l'échelle parcellaire."
  ),
  SMOKE: D(
    "Deux modèles de composition atmosphérique restituent le panache de fumée d'un incendie — la concentration de particules fines (PM2.5) transportée par les vents. CAMS (Copernicus/ECMWF) est mis à jour quotidiennement : c'est la source pour les feux en cours. GEOS-CF (NASA) est plus fin mais son archive Earth Engine s'arrête début 2026 : à réserver aux études de cas passées. Les deux ingèrent les émissions des feux et se lisent en animation, jour par jour.",
    "PM2.5 = aérosols fins (carbone suie, carbone organique, poussière, sulfate, sel de mer). CAMS fournit aussi l'épaisseur optique d'aérosols (AOD), sans dimension.",
    "PM2.5 en µg/m³ à la surface. Repères qualité de l'air : < 12 bon, 12–35 modéré, 35–55 mauvais pour les personnes sensibles, 55–150 mauvais, > 150 très mauvais à dangereux (fumée dense). L'AOD croît avec l'épaisseur du panache (fumée dense au-delà de 1).",
    "Modèles à 27–44 km : un panache est une masse régionale, pas un front de feu à l'échelle parcellaire. GEOS-CF v1 surestime les aérosols de 20–50 %, et sa couverture GEE s'arrête ~2026 (pas de temps réel) — pour un feu actuel, utiliser CAMS. Pour la fumée observée par satellite en direct, croiser avec l'indice d'aérosols Sentinel-5P."
  ),
  LST: D(
    "La température de surface est celle de la peau du sol ou de la canopée vue depuis l'espace — distincte de la température de l'air mesurée sous abri. Elle révèle les îlots de chaleur urbains, le stress thermique des cultures et l'activité volcanique.",
    null,
    "Affichée en degrés Celsius. En journée d'été, une surface minérale dépasse couramment de 15 à 20 °C la température de l'air.",
    "Mesure impossible sous les nuages : les pixels nuageux sont masqués. Une moyenne calculée sur peu d'images claires peut être biaisée vers les journées de beau temps."
  ),
  AIRTEMP: D(
    "Température de l'air à 2 mètres issue de la réanalyse ERA5-Land, qui combine modèle atmosphérique et observations pour produire une série homogène depuis 1950 — la référence pour les comparaisons climatiques de long terme.",
    null,
    "En degrés Celsius, moyenne mensuelle.",
    "Maille d'environ 11 km : les contrastes locaux, relief et effets urbains sont lissés."
  ),
  PRECIP: D(
    "CHIRPS combine imagerie infrarouge thermique et relevés de stations pour fournir des précipitations quotidiennes depuis 1981. Conçu pour le suivi des sécheresses, il est particulièrement utilisé en Afrique et dans les régions à réseau de mesure clairsemé.",
    null,
    "En millimètres, cumulés sur la période demandée.",
    "Un cumul dépend de la longueur de la période choisie : comparer des durées égales, sans quoi le résultat n'a aucun sens."
  ),
  GPMPRECIP: D(
    "GPM IMERG fusionne l'ensemble de la constellation de satellites de mesure des précipitations, radar et micro-ondes, pour une couverture mondiale homogène incluant les océans — là où CHIRPS s'arrête aux terres émergées.",
    null,
    "En millimètres par heure, moyenne mensuelle. Multiplier par la durée pour un cumul.",
    "Sous-estime les précipitations neigeuses et les pluies orographiques de relief."
  ),
  ET: D(
    "L'évapotranspiration additionne l'évaporation des sols et la transpiration des plantes. C'est le terme de sortie du bilan hydrique, utilisé pour piloter l'irrigation et estimer la consommation en eau des cultures.",
    null,
    "En kilogrammes par mètre carré et par période de 8 jours, numériquement égaux à des millimètres.",
    "Produit modélisé et non mesuré ; sa qualité dépend des données météorologiques d'entrée et de la classification du couvert."
  ),
  SOLAR: D(
    "Rayonnement solaire incident à la surface, issu d'ERA5-Land. Donnée d'entrée du dimensionnement photovoltaïque et des modèles agronomiques.",
    null,
    "En joules par mètre carré cumulés sur le mois ; diviser par la durée pour obtenir une puissance moyenne.",
    "Issu d'une réanalyse : la nébulosité locale peut être imparfaitement restituée."
  ),
  WIND: D(
    "Vitesse du vent à 10 mètres, calculée depuis les composantes zonale et méridienne d'ERA5-Land. Sert au repérage préliminaire de sites éoliens et à l'analyse des régimes de vent.",
    null,
    "En mètres par seconde, moyenne mensuelle.",
    "À 11 km de maille et 10 m de hauteur, la donnée ne remplace pas une campagne de mesure au moyeu d'éolienne, qui se situe bien plus haut."
  ),
  SNOW: D(
    "Couverture neigeuse quotidienne dérivée de MODIS, à la base des suivis d'enneigement et des prévisions de fonte pour l'hydrologie de montagne.",
    null,
    "Indice NDSI de couverture de 0 à 100 ; au-delà de 40, le pixel est généralement considéré enneigé.",
    "Un couvert forestier dense masque la neige au sol, qui est alors sous-estimée."
  ),
  NO2: D(
    "Colonne de dioxyde d'azote mesurée par TROPOMI. Ce polluant provient de la combustion — trafic routier, centrales, industrie — et sa courte durée de vie atmosphérique en fait un traceur direct des sources actives.",
    null,
    "Colonne troposphérique en mol/m². Les axes routiers et zones industrielles ressortent nettement.",
    "Il s'agit d'une colonne intégrée sur toute l'épaisseur atmosphérique, non d'une concentration respirable au sol."
  ),
  CO: D(
    "Colonne de monoxyde de carbone, traceur des combustions incomplètes : feux de biomasse, chauffage domestique, trafic. Sa durée de vie de plusieurs semaines permet de suivre les panaches sur de longues distances.",
    null, "Colonne totale en mol/m².",
    "Fond naturel élevé en région de feux saisonniers : interpréter les écarts, non les valeurs brutes."
  ),
  CH4: D(
    "Colonne de méthane, deuxième gaz à effet de serre d'origine humaine. Les sources majeures sont l'extraction d'hydrocarbures, les décharges, les rizières et l'élevage.",
    null, "Fraction molaire sèche en parties par milliard (ppb).",
    "Mesure impossible au-dessus de l'eau et des surfaces sombres ; nombreuses lacunes sur les océans et aux hautes latitudes."
  ),
  O3: D(
    "Colonne totale d'ozone. Protecteur en haute atmosphère où il filtre l'ultraviolet, il devient polluant près du sol lors des pics estivaux.",
    null, "Colonne totale en mol/m².",
    "Le signal est dominé par l'ozone stratosphérique : cette mesure ne renseigne pas sur la qualité de l'air respiré."
  ),
  AER: D(
    "Indice d'aérosols ultraviolet, sensible aux particules absorbantes : poussières désertiques, fumées d'incendie, cendres volcaniques.",
    null, "Indice sans unité ; les valeurs positives signalent la présence d'aérosols absorbants.",
    "Ne distingue pas la nature des particules : sable saharien et fumée d'incendie donnent un signal comparable."
  ),
  NDBI: D(
    "Indice de bâti : les surfaces artificialisées réfléchissent davantage dans le moyen infrarouge que dans le proche infrarouge, à l'inverse de la végétation. Utilisé pour cartographier l'emprise urbaine et suivre l'étalement.",
    "(MIR₁ − PIR) / (MIR₁ + PIR)",
    "Positif sur bâti et sol nu, négatif sur végétation et eau.",
    "Ne sépare pas le bâti du sol nu, qui répond de la même façon : sur zone aride, préférer un produit d'occupation du sol."
  ),
  BSI: D(
    "Indice de sol nu, combinant quatre bandes pour isoler les surfaces dépourvues de végétation. Sert au suivi de l'érosion, de la dégradation des terres et des chantiers.",
    "((MIR₁ + Rouge) − (PIR + Bleu)) / ((MIR₁ + Rouge) + (PIR + Bleu))",
    "Croît avec la proportion de sol nu.",
    "Sensible à la couleur et à l'humidité du sol : un sol sombre ou détrempé abaisse l'indice sans que le couvert change."
  ),
  NBR: D(
    "Ratio de brûlage normalisé, conçu pour délimiter les surfaces incendiées et graduer la sévérité du feu. La pratique courante consiste à différencier une image avant et une après incendie (dNBR).",
    "(PIR − MIR₂) / (PIR + MIR₂)",
    "Élevé sur végétation saine, fortement négatif sur surface récemment brûlée. C'est l'écart avant/après qui mesure la sévérité.",
    "Une seule date ne suffit pas à conclure : sol nu et surface brûlée ancienne donnent des valeurs proches."
  ),
  NDSI: D(
    "Indice de neige : la neige réfléchit fortement le vert et absorbe le moyen infrarouge, contrairement aux nuages — ce qui permet précisément de les distinguer.",
    "(Vert − MIR₁) / (Vert + MIR₁)",
    "Au-delà d'environ 0,4, le pixel est généralement considéré enneigé.",
    "Formule identique au MNDWI : l'eau libre ressort aussi positive, à masquer avant interprétation."
  ),
  WORLDCOVER: D(
    "Carte mondiale d'occupation du sol à 10 mètres produite par l'ESA à partir de Sentinel-1 et Sentinel-2, en onze classes. Référence courante pour les analyses de paysage et le calage de statistiques territoriales.",
    null,
    "Classes discrètes : arbres, arbustes, prairies, cultures, bâti, sol nu, neige, eau, zone humide, mangrove, lichen.",
    "Donnée catégorielle : moyenne, écart-type et classification par quantiles n'ont aucun sens sur ces valeurs, qui sont des étiquettes."
  ),
  DYNWORLD: D(
    "Occupation du sol quasi temps réel produite par un réseau de neurones appliqué à chaque image Sentinel-2. Contrairement aux cartes annuelles, elle permet de suivre des changements en cours de saison.",
    null,
    "Neuf classes ; sur une période, la classe majoritaire est retenue pour chaque pixel.",
    "Une classification par image est plus bruitée qu'un produit annuel consolidé : allonger la période stabilise le résultat."
  ),
  POPULATION: D(
    "Population maillée du GHSL, obtenue en redistribuant les recensements nationaux au prorata du bâti détecté par satellite. Base des analyses d'exposition aux risques et d'accès aux services.",
    null,
    "Nombre d'habitants par cellule de 100 m. Sommer sur une emprise donne la population totale.",
    "Une désagrégation reste une estimation : la précision dépend de la finesse du recensement d'origine. Les époques 2025 et 2030 sont des projections."
  ),
  BUILT: D(
    "Surface bâtie détectée par satellite depuis 1975, par pas de cinq ans. La profondeur temporelle en fait la source de référence pour mesurer l'artificialisation et l'étalement urbain sur cinquante ans.",
    null,
    "Mètres carrés bâtis par cellule de 100 m ; rapporté à la surface de la cellule, on obtient un taux d'emprise au sol.",
    "Mesure l'emprise au sol, pas le volume construit : une tour et un hangar de même empreinte comptent pareil."
  ),
  SMOD: D(
    "Degré d'urbanisation, classification officiellement adoptée par l'Union européenne et l'ONU pour comparer villes et campagnes entre pays sur une définition commune, indépendante des découpages administratifs nationaux.",
    null,
    "Classes de 10 à 30 : zones rurales dispersées, villages, périurbain dense, clusters urbains, centres urbains.",
    "Donnée catégorielle : ne pas moyenner. Les seuils sont fixés par la méthode officielle, non ajustables."
  ),
  VIIRS: D(
    "Lumières nocturnes mensuelles. Servent d'approximation de l'activité économique et de l'électrification, et permettent de suivre les interruptions de service après une catastrophe.",
    null,
    "Radiance en nW/cm²/sr ; la dynamique est très étalée, un rendu logarithmique est souvent plus lisible.",
    "La conversion massive vers l'éclairage LED, plus bleu, réduit le signal capté sans que l'éclairage réel diminue — attention aux tendances de long terme."
  ),
  BURNED: D(
    "Surfaces brûlées mensuelles détectées par MODIS depuis 2000, à partir des changements de réflectance consécutifs au passage du feu. Complète la détection de foyers actifs, qui ne voit que l'instant de la combustion.",
    null,
    "La valeur est le jour de l'année de détection du brûlage, non une intensité.",
    "Résolution de 500 m : les petits feux agricoles passent largement inaperçus."
  ),
  FIRMS: D(
    "Foyers actifs détectés en quasi temps réel par leur signature thermique. Outil de suivi opérationnel des incendies en cours.",
    null,
    "Température de brillance du canal 4 µm : plus elle est élevée, plus le foyer est intense.",
    "Ne voit que ce qui brûle au moment du passage du satellite. Torchères industrielles et cheminées produisent de fausses détections."
  ),
  SOC: D(
    "Carbone organique du sol, déterminant de la fertilité, de la capacité de rétention en eau et de la structure. Le sol constitue le principal réservoir de carbone terrestre, devant la végétation.",
    null, "En grammes par kilogramme de terre fine.",
    "Prédiction spatialisée par apprentissage à partir de profils ponctuels : fiable en tendance régionale, à vérifier par analyse pour une parcelle donnée."
  ),
  SOILPH: D(
    "pH du sol mesuré dans l'eau. Il commande la disponibilité des nutriments et le choix des cultures.",
    null, "De 3,5 (très acide) à 9 (très basique) ; la plupart des cultures se situent entre 6 et 7,5.",
    "La donnée source est encodée en pH × 10 ; c'est aussi une prédiction, non une mesure de terrain."
  ),
  CLAY: D(
    "Teneur en argile, qui gouverne la rétention en eau, la capacité d'échange cationique et le comportement mécanique du sol.",
    null, "Pourcentage massique de la fraction fine.",
    "Comme les autres variables pédologiques, il s'agit d'une prédiction cartographique et non d'une analyse d'échantillon."
  ),
  BATHY: D(
    "ETOPO1 combine relief terrestre et bathymétrie océanique dans un modèle mondial unique — utile pour le contexte physiographique et les cartes à petite échelle.",
    null, "Mètres, négatifs sous le niveau de la mer.",
    "Résolution kilométrique : impropre à toute navigation ou étude côtière fine."
  ),
  ELEV: D(
    "Modèle numérique d'élévation, socle de toute analyse de terrain : pentes, expositions, bassins versants, visibilité, modélisation d'inondation.",
    null, "Altitude en mètres au-dessus du géoïde.",
    "SRTM et Copernicus DEM sont des modèles de SURFACE : ils incluent canopée et bâti. En forêt, l'altitude est celle du sommet des arbres, pas du sol."
  ),
  SLOPE: D(
    "Pente calculée à partir du modèle d'élévation. Entre dans l'évaluation du risque d'érosion, l'aptitude à la mise en culture, le tracé d'infrastructures et l'analyse d'aléa gravitaire.",
    null, "En degrés, de 0 (plat) à 90 (vertical).",
    "La pente dépend de la résolution du modèle : à 30 m, les ruptures fines sont lissées et les valeurs extrêmes minorées."
  ),
  HILLSHADE: D(
    "Ombrage porté simulant l'éclairement du relief par une source lumineuse d'azimut et de hauteur choisis. Rendu de lisibilité destiné à faire percevoir les formes du terrain.",
    null, "Valeurs de 0 (ombre portée) à 255 (face pleinement éclairée).",
    "Rendu visuel et non mesure physique : ne pas en tirer de statistiques. L'exagération verticale accentue les formes mais fausse les proportions réelles."
  ),
  // ── Océanographie ──
  SST: D(
    "Température de surface de la mer : la couche de peau de l'océan mesurée depuis l'orbite dans l'infrarouge thermique (MODIS) ou reconstruite sans trous par interpolation optimale de satellites et bouées (OISST). C'est la variable de base de l'océanographie physique — moteur des échanges océan-atmosphère, marqueur d'El Niño, des upwellings et des canicules marines.",
    "MODIS : bande SST (11 µm). OISST : analyse interpolée quotidienne.",
    "En °C. Eaux polaires proches de −2 ; tempérées 10–20 ; tropicales 25–30. Les fronts thermiques marquent souvent des zones biologiquement riches.",
    "MODIS voit la peau (~10 µm d'épaisseur), pas la température du volume d'eau, et laisse des trous sous les nuages. OISST comble les trous mais lisse le côtier à ~27 km : préférer MODIS près des côtes, OISST pour les séries longues."
  ),
  SSTANOM: D(
    "Écart de la température de surface à sa normale climatologique. C'est l'indicateur direct des vagues de chaleur marines : un excès prolongé de SST qui blanchit les coraux, déplace les stocks halieutiques et bouleverse les écosystèmes. Le champ d'anomalie fait ressortir l'événement là où la SST brute ne montrerait qu'une carte saisonnière ordinaire.",
    "SST observée − moyenne climatologique (fournie par OISST)",
    "En °C, centrée sur 0. Rouge = plus chaud que la normale (canicule marine si > +1 sur plusieurs semaines) ; bleu = plus froid (souvent upwelling ou La Niña).",
    "Une anomalie n'est qu'un écart à une référence : sa valeur dépend de la période climatologique retenue. Un pic ponctuel n'est pas une canicule marine — c'est la persistance dans le temps qui compte."
  ),
  CHLORO: D(
    "Concentration en chlorophylle-a, pigment du phytoplancton : c'est le proxy de la vie végétale de l'océan et donc de la base des chaînes alimentaires marines. La couleur de l'eau vue du satellite — plus verte quand le phytoplancton abonde — permet de cartographier la productivité, les efflorescences et les zones de pêche.",
    "Algorithme couleur de l'eau OCx (rapports de réflectances bleu/vert)",
    "En mg/m³, très étalée : eaux tropicales oligotrophes < 0,1 ; plateaux et upwellings 1–10 ; efflorescences côtières > 20. Une échelle logarithmique ou une classification par quantiles est presque toujours nécessaire.",
    "En eaux côtières troubles (« cas 2 »), sédiments et matière organique dissoute font surestimer la chlorophylle. Le produit ne concerne que la couche de surface éclairée, pas la colonne d'eau entière."
  ),
  TURBIDITY: D(
    "Charge en matières particulaires des eaux de surface, approchée ici par le carbone organique particulaire (POC). Renseigne sur les panaches fluviaux, la remise en suspension des sédiments et la qualité des eaux côtières. Complète la chlorophylle pour distinguer eau claire productive et eau chargée.",
    "Produit POC de la couleur de l'eau (proxy de la turbidité)",
    "En mg/m³ : faible au large, forte près des embouchures et sur les petits fonds remaniés par la houle.",
    "C'est un PROXY, pas une mesure de turbidité normalisée (NTU). Pour le suivi fin d'un estuaire, l'indice NDCI sur Sentinel-2 (10 m) est plus adapté que ces 4 km."
  ),
  CURRENTS: D(
    "Vitesse des courants marins de surface, issue du modèle océanique HYCOM qui assimile observations satellitaires et in situ. Structure la dispersion de la chaleur, des nutriments, des larves et des polluants ; encadre la navigation et la recherche en mer.",
    "‖(u, v)‖ des composantes de vitesse à 0 m",
    "En m/s. Océan ouvert calme < 0,2 ; courants de bord (Gulf Stream, Kuroshio, Agulhas) 1–2. La carte de vitesse seule ne donne pas la direction.",
    "Sortie de MODÈLE, pas une observation directe : fiable sur les grandes structures, moins sur les tourbillons fins et le très côtier. On n'affiche que la vitesse ; la direction (u, v) n'est pas représentée en flèches ici."
  ),
  OCEANWIND: D(
    "Vitesse du vent à 10 m au-dessus des océans, tirée de la réanalyse ERA5. Le vent force les vagues, les courants de surface et les upwellings, et conditionne les échanges de chaleur et de gaz avec l'atmosphère. Support du potentiel éolien offshore et de l'analyse des tempêtes.",
    "‖(u₁₀, v₁₀)‖ — module des composantes du vent à 10 m, moyenné sur la période",
    "En m/s. Alizés réguliers 5–10 ; quarantièmes rugissants et tempêtes > 15. La moyenne sur la période lisse rafales et cyclones.",
    "Il s'agit d'une réanalyse (modèle contraint par les observations), pas d'une mesure de diffusiomètre. La moyenne sur la période masque les extrêmes — élargir la fenêtre lisse d'autant plus."
  ),
  SALINITY: D(
    "Salinité des eaux de surface, fournie par le modèle HYCOM. Avec la température, elle contrôle la densité de l'eau de mer et donc la circulation thermohaline. Ses contrastes révèlent panaches fluviaux, zones d'évaporation intense et fonte des glaces.",
    "Salinité HYCOM à 0 m (facteur 0,001, décalage +20)",
    "En PSU (unités pratiques de salinité). Océan ouvert 34–37 ; abaissée près des embouchures et sous la banquise en fonte ; élevée en Méditerranée et mer Rouge (> 38).",
    "Sortie de modèle assimilé, non une mesure directe : elle remplace les capteurs satellitaires de salinité (SMOS, Aquarius) absents de GEE, mais n'en a ni la nature ni les incertitudes propres."
  ),
  SEAICE: D(
    "Concentration de la glace de mer : fraction d'un pixel océanique couverte de banquise. Indicateur climatique majeur — l'étendue des glaces polaires est l'un des signaux les plus nets du réchauffement — et contrainte directe pour la navigation et les écosystèmes polaires.",
    "Fraction de glace OISST, exprimée en %",
    "De 0 (eau libre) à 100 % (couverture totale). La limite de banquise se lit au gradient entre 15 % et 80 %.",
    "OISST donne la CONCENTRATION, pas l'épaisseur ni le volume — deux banquises à 100 % peuvent avoir des masses très différentes. À ~27 km, le trait de côte glacé est approximatif."
  ),
  CORAL: D(
    "Emprise des récifs coralliens cartographiée par l'Allen Coral Atlas à partir d'imagerie satellitaire haute résolution. Support de la conservation marine : localisation des habitats, suivi du blanchissement, planification des aires protégées.",
    "Masque récifal (habitat benthique/géomorphologique classé)",
    "Couche de présence : les pixels marqués correspondent à un récif cartographié. Les classes détaillées (récif externe, lagon, herbier…) existent dans la donnée source.",
    "État de référence 2018–2020 : ne reflète pas les évolutions récentes ni les épisodes de blanchissement postérieurs. La cartographie s'arrête aux eaux peu profondes et claires des ceintures tropicales."
  ),
  MANGROVE: D(
    "Forêts de mangrove de l'an 2000, dérivées de Landsat. Écosystèmes côtiers à très forte valeur : puits de carbone « bleu », nurseries halieutiques, protection des littoraux contre l'érosion et les submersions.",
    "Masque de présence des mangroves (Giri et al., 2011)",
    "Couche binaire : présence/absence à 30 m pour l'année 2000.",
    "Instantané de l'an 2000 : ni les pertes (déforestation, aquaculture) ni les gains ultérieurs n'y figurent. Pour un état récent, croiser avec Global Forest Watch ou une classification Sentinel-2 à jour."
  ),
  // ── GEDI — LiDAR spatial ──
  GEDI_CANOPY: D(
    "Hauteur de la canopée mesurée par le LiDAR spatial GEDI, à bord de la Station spatiale internationale. En envoyant une impulsion laser et en analysant l'écho, l'instrument mesure directement la structure verticale de la végétation — là où les indices optiques (NDVI) ne voient que la densité. C'est la référence pour la hauteur des forêts et l'estimation du carbone.",
    "RH98 — hauteur sous laquelle reviennent 98 % de l'énergie LiDAR",
    "En mètres. Prairies et cultures sous 3 ; forêts tempérées 15–35 ; forêts tropicales denses au-delà de 40.",
    "Échantillonnage par EMPREINTES de 25 m le long des traces orbitales, non une couverture continue : de nombreux pixels sont vides, surtout sur une période courte. Élargissez la période pour densifier. Aucune donnée au-delà de ±51,6° de latitude (les hautes latitudes boréales sont exclues)."
  ),
  GEDI_ELEV: D(
    "Altitude du sol restituée par GEDI sous le couvert végétal. Le dernier écho de l'onde LiDAR provient du sol, ce qui donne une élévation du terrain même en forêt dense — utile là où les MNT optiques captent le sommet de la canopée.",
    "elev_lowestmode — élévation du mode le plus bas de l'onde",
    "En mètres au-dessus de l'ellipsoïde. À comparer aux MNT SRTM/Copernicus pour estimer la hauteur du couvert.",
    "Même échantillonnage épars par empreintes que la hauteur de canopée, et même limite de latitude ±51,6°."
  ),
  GEDI_COVER: D(
    "Fraction de couvert arboré vue par le LiDAR : la proportion du sol interceptée par la végétation au-dessus d'un seuil de hauteur. Complète la hauteur pour décrire l'ouverture d'un peuplement.",
    "cover — fraction de couvert (0–1), affichée en %",
    "De 0 (sol nu) à 100 % (couvert fermé). Une forêt claire de savane peut être haute mais peu couvrante.",
    "Produit L2B, échantillonné par empreintes et limité à ±51,6° de latitude comme le reste de GEDI."
  ),
  GEDI_PAI: D(
    "Indice de surface foliaire végétale (Plant Area Index) mesuré par GEDI : la surface totale d'éléments végétaux (feuilles et branches) par unité de surface au sol. Décrit la densité du feuillage et alimente les modèles de productivité et de biomasse.",
    "pai — surface d'éléments végétaux par surface au sol",
    "En m²/m². Végétation clairsemée sous 1 ; forêts denses 4–8.",
    "Inclut les branches (contrairement au LAI purement foliaire). Échantillonnage épars par empreintes, ±51,6° de latitude."
  ),
  GEDI_AGB: D(
    "Densité de biomasse aérienne estimée par GEDI à la résolution de l'empreinte (25 m), à partir des métriques de hauteur du LiDAR. Plus fine que le produit grillé à 1 km, elle sert à cartographier le stock de carbone forestier et à suivre la dégradation.",
    "agbd — modèle biomasse ↔ métriques de hauteur (par biome)",
    "En mégagrammes par hectare (Mg/ha, = tonnes/ha). Savanes sous 50 ; forêts tempérées 100–300 ; tropicales denses au-delà de 400.",
    "Estimation par MODÈLE, pas une mesure directe : l'incertitude par empreinte est élevée. Couverture éparse (empreintes) et limitée à ±51,6° de latitude. Pour un état continu, le produit grillé L4B (1 km) reste plus complet."
  ),
};

/** Fiche complète d'une option d'indicateur (croise doc + métadonnées source). */
export function docFor(indicatorKey, dataset) {
  return { doc: INDICATOR_DOC[indicatorKey] || null, source: SOURCE_META[dataset] || null };
}
