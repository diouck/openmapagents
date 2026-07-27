/**
 * IndexStatsModal.jsx
 * Modale flottante draggable — statistiques GEE pour tous les indices.
 * Supporte : NDVI, NDWI, NDBI, EVI, LST Jour/Nuit/température,
 *            MODIS NDVI/EVI, ESA WorldCover, Hansen Forest Watch,
 *            ERA5 Temp/Précip/Humidité, SRTM Élévation/Pente/Ombrage,
 *            Hauteur canopée WRI/Meta
 */
import { useState, useEffect, useCallback, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { IcX, IcLoader, IcMove } from "../icons";
import { nextZ, bumpZ } from "../utils/zorder";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ══════════════════════════════════════════════════════════════
// CONFIG PAR INDEX
// ══════════════════════════════════════════════════════════════
const INDEX_CONFIG = {

  NDVI: {
    icon: "🌿", label: "NDVI",
    gradient: ["#d73027","#f46d43","#fdae61","#fee08b","#d9ef8b","#a6d96a","#66bd63","#1a9850"],
    gradientLabels: ["-1 (eau/sol)", "0", "+1 (végétation dense)"],
    statCards: (s) => [
      { label: "NDVI moyen",   value: s.mean?.toFixed(2), unit: "", accent: true },
      { label: "NDVI max",     value: s.max?.toFixed(2),  unit: "" },
      { label: "Végétation",   value: s.veg_pct?.toFixed(0), unit: "%" },
      { label: "Surface vég.", value: s.veg_km2 >= 1 ? s.veg_km2?.toFixed(1) : (s.veg_km2 * 100)?.toFixed(0), unit: s.veg_km2 >= 1 ? "km²" : "ha" },
    ],
    distTitle: "Distribution des valeurs NDVI",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe de végétation",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: (s) => s.cloud_max != null ? ` · Nuages : < ${s.cloud_max}%` : "",
  },

  NDWI: {
    icon: "💧", label: "NDWI",
    gradient: ["#8B4513","#DEB887","#ffffff","#AED6F1","#2171b5","#084594"],
    gradientLabels: ["-1 (sec/sol)", "0", "+1 (eau libre)"],
    statCards: (s) => [
      { label: "NDWI moyen",  value: s.mean?.toFixed(2),  unit: "", accent: true },
      { label: "NDWI max",    value: s.max?.toFixed(2),   unit: "" },
      { label: "Surface eau", value: s.water_pct?.toFixed(0), unit: "%" },
      { label: "Eau libre",   value: s.water_km2 >= 1 ? s.water_km2?.toFixed(1) : (s.water_km2 * 100)?.toFixed(0), unit: s.water_km2 >= 1 ? "km²" : "ha" },
    ],
    distTitle: "Distribution des valeurs NDWI",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe d'humidité",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: (s) => s.cloud_max != null ? ` · Nuages : < ${s.cloud_max}%` : "",
  },

  NDBI: {
    icon: "🏙", label: "NDBI",
    gradient: ["#1a9850","#a6d96a","#fee08b","#f46d43","#d73027"],
    gradientLabels: ["-1 (végétation)", "0", "+1 (bâti/urbain)"],
    statCards: (s) => [
      { label: "NDBI moyen",      value: s.mean?.toFixed(2),  unit: "", accent: true },
      { label: "NDBI max",        value: s.max?.toFixed(2),   unit: "" },
      { label: "Surface urbaine", value: s.urban_pct?.toFixed(0), unit: "%" },
      { label: "Zone bâtie",      value: s.urban_km2 >= 1 ? s.urban_km2?.toFixed(1) : (s.urban_km2 * 100)?.toFixed(0), unit: s.urban_km2 >= 1 ? "km²" : "ha" },
    ],
    distTitle: "Distribution des valeurs NDBI",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe d'urbanisation",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: (s) => s.cloud_max != null ? ` · Nuages : < ${s.cloud_max}%` : "",
  },

  EVI: {
    icon: "🌱", label: "EVI",
    gradient: ["#d73027","#fdae61","#d9ef8b","#1a9850"],
    gradientLabels: ["-0.2 (sol/eau)", "0.2", "+0.8 (dense)"],
    statCards: (s) => [
      { label: "EVI moyen",  value: s.mean?.toFixed(2),  unit: "", accent: true },
      { label: "EVI max",    value: s.max?.toFixed(2),   unit: "" },
      { label: "Végétation", value: s.veg_pct?.toFixed(0), unit: "%" },
      { label: "Biomasse",   value: s.biomass_label || "–", unit: "" },
    ],
    distTitle: "Distribution des valeurs EVI",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par niveau de végétation",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: (s) => s.cloud_max != null ? ` · Nuages : < ${s.cloud_max}%` : "",
  },

  "LST Jour": {
    icon: "🌡", label: "LST Jour",
    gradient: ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],
    gradientLabels: ["Froid", "Tempéré", "Chaud"],
    statCards: (s) => [
      { label: "Temp. moy. jour", value: s.mean != null ? `${s.mean.toFixed(1)}°` : "–", unit: "C", accent: true },
      { label: "Min", value: s.min != null ? `${s.min.toFixed(1)}°` : "–", unit: "C" },
      { label: "Max", value: s.max != null ? `${s.max.toFixed(1)}°` : "–", unit: "C" },
      { label: "Amplitude", value: s.amplitude != null ? `${s.amplitude.toFixed(1)}°` : "–", unit: "C" },
    ],
    distTitle: "Distribution des températures (jour)",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe thermique",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => "",
  },

  "LST Nuit": {
    icon: "🌙", label: "LST Nuit",
    gradient: ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],
    gradientLabels: ["Froid", "Tempéré", "Chaud"],
    statCards: (s) => [
      { label: "Temp. moy. nuit", value: s.mean != null ? `${s.mean.toFixed(1)}°` : "–", unit: "C", accent: true },
      { label: "Min", value: s.min != null ? `${s.min.toFixed(1)}°` : "–", unit: "C" },
      { label: "Max", value: s.max != null ? `${s.max.toFixed(1)}°` : "–", unit: "C" },
      { label: "Amplitude", value: s.amplitude != null ? `${s.amplitude.toFixed(1)}°` : "–", unit: "C" },
    ],
    distTitle: "Distribution des températures (nuit)",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe thermique",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => "",
  },

  "LST (température)": {
    icon: "🌡", label: "LST Landsat",
    gradient: ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],
    gradientLabels: ["Froid", "Tempéré", "Chaud"],
    statCards: (s) => [
      { label: "Temp. moyenne", value: s.mean != null ? `${s.mean.toFixed(1)}°` : "–", unit: "C", accent: true },
      { label: "Min", value: s.min != null ? `${s.min.toFixed(1)}°` : "–", unit: "C" },
      { label: "Max", value: s.max != null ? `${s.max.toFixed(1)}°` : "–", unit: "C" },
      { label: "Amplitude", value: s.amplitude != null ? `${s.amplitude.toFixed(1)}°` : "–", unit: "C" },
    ],
    distTitle: "Distribution des températures de surface",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe thermique",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: (s) => s.cloud_max != null ? ` · Nuages : < ${s.cloud_max}%` : "",
  },

  "Température air": {
    icon: "🌤", label: "Température air ERA5",
    gradient: ["#040274","#3288bd","#abdda4","#fdae61","#d53e4f","#9e0142"],
    gradientLabels: ["Très froid", "Tempéré", "Chaud"],
    statCards: (s) => [
      { label: "Temp. moyenne", value: s.mean != null ? `${s.mean.toFixed(1)}°` : "–", unit: "C", accent: true },
      { label: "Minimum", value: s.min != null ? `${s.min.toFixed(1)}°` : "–", unit: "C" },
      { label: "Maximum", value: s.max != null ? `${s.max.toFixed(1)}°` : "–", unit: "C" },
      { label: "Amplitude", value: s.amplitude != null ? `${s.amplitude.toFixed(1)}°` : "–", unit: "C" },
    ],
    distTitle: "Distribution des températures (mensuelle)",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par zone thermique",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · Résol. : 11 km · Mensuel",
  },

  "Précipitations": {
    icon: "🌧", label: "Précipitations ERA5",
    gradient: ["#f7fbff","#c6dbef","#6baed6","#2171b5","#084594"],
    gradientLabels: ["0 mm (sec)", "Modéré", "Intense"],
    statCards: (s) => [
      { label: "Total mensuel",    value: s.total_mm?.toFixed(0),   unit: "mm", accent: true },
      { label: "Moy. journalière", value: s.daily_mean?.toFixed(1), unit: "mm" },
      { label: "Jours de pluie",   value: s.rainy_days?.toFixed(0), unit: "j"  },
      { label: "Max journalier",   value: s.max_daily?.toFixed(1),  unit: "mm" },
    ],
    distTitle: "Distribution des précipitations",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe pluviométrique",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · Résol. : 11 km · Mensuel",
  },

  "Humidité": {
    icon: "💦", label: "Humidité ERA5",
    gradient: ["#f7fbff","#c6dbef","#6baed6","#2171b5","#084594"],
    gradientLabels: ["Sec (0%)", "Modéré", "Saturé (100%)"],
    statCards: (s) => [
      { label: "Humidité moy.", value: s.mean?.toFixed(0), unit: "%", accent: true },
      { label: "Minimum",       value: s.min?.toFixed(0),  unit: "%" },
      { label: "Maximum",       value: s.max?.toFixed(0),  unit: "%" },
      { label: "Variabilité",   value: s.std?.toFixed(1),  unit: "σ" },
    ],
    distTitle: "Distribution de l'humidité",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe d'humidité",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · Résol. : 11 km · Mensuel",
  },

  // ── ESA WorldCover ────────────────────────────────────────
  "Occupation du sol": {
    icon: "🗺", label: "Occupation du sol",
    gradient: [], gradientLabels: [],
    noGradient: true,
    statCards: (s) => [
      { label: "Classes",      value: s.n_classes?.toString(),  unit: ""    },
      { label: "Végétation",   value: s.veg_pct?.toFixed(0),    unit: "%", accent: true },
      { label: "Surface tot.", value: s.total_km2?.toFixed(1),  unit: "km²" },
      { label: "Artificiel",   value: s.urban_pct?.toFixed(0),  unit: "%"   },
    ],
    distTitle: "Répartition par classe d'occupation du sol",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Grandes catégories",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: (s) => ` · Millésime : ${s.year || 2021}`,
  },

  // ── Hansen Forest Watch ───────────────────────────────────
  "Couverture forêt 2000": {
    icon: "🌳", label: "Forêt & Déforestation",
    gradient: ["#d9ef8b","#66bd63","#238443","#005a32"],
    gradientLabels: ["0% (sans arbre)", "50%", "100% (dense)"],
    statCards: (s) => [
      { label: "Couvert 2000", value: s.cover_pct?.toFixed(0), unit: "%", accent: true },
      { label: "Perte 00–23",  value: s.loss_pct?.toFixed(0),  unit: "%", accentRed: true },
      { label: "Gain forêt",   value: s.gain_pct?.toFixed(0),  unit: "%"  },
      { label: "Forêt nette",  value: s.net_pct?.toFixed(0),   unit: "%"  },
    ],
    distTitle: "Couverture arborée en 2000 (% canopée)",
    distClasses: (s) => s.classes || [],
    dist2Title: "Perte forestière par décennie",
    dist2Classes: (s) => s.loss_by_decade || [],
    surfaceTitle: "Bilan forestier",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · Référence : Hansen v1.11",
  },

  "Perte forêt": {
    icon: "🌳", label: "Perte forêt (Hansen)",
    gradient: ["#ffffe5","#78c679","#004529"],
    gradientLabels: ["Aucune perte", "2010", "2023"],
    statCards: (s) => [
      { label: "Perte totale",   value: s.loss_pct?.toFixed(0),  unit: "%", accentRed: true },
      { label: "Surface perdue", value: s.loss_km2?.toFixed(1),  unit: "km²" },
      { label: "Pire année",     value: s.worst_year?.toString(), unit: "" },
      { label: "Tendance",       value: s.trend || "–",           unit: "" },
    ],
    distTitle: "Perte forestière par année",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Perte par décennie",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · Référence : Hansen v1.11",
  },

  "Gain forêt": {
    icon: "🌿", label: "Gain forêt (Hansen)",
    gradient: ["#ffffff","#00ff00"],
    gradientLabels: ["Aucun gain", "Gain maximal"],
    statCards: (s) => [
      { label: "Gain total",     value: s.gain_pct?.toFixed(0),  unit: "%", accent: true },
      { label: "Surface gagnée", value: s.gain_km2?.toFixed(1),  unit: "km²" },
      { label: "Bilan net",      value: s.net_pct != null ? `${s.net_pct > 0 ? "+" : ""}${s.net_pct.toFixed(0)}` : "–", unit: "%" },
      { label: "Statut",         value: s.status || "–",          unit: "" },
    ],
    distTitle: "Distribution du gain forestier",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Zones de reboisement",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · Référence : Hansen v1.11",
  },

  // ── SRTM ─────────────────────────────────────────────────
  "Élévation": {
    icon: "⛰", label: "Élévation SRTM",
    gradient: ["#313695","#74add1","#e0f3f8","#fee090","#f46d43","#a50026"],
    gradientLabels: ["0 m (mer)", "500 m", "> 3000 m"],
    statCards: (s) => [
      { label: "Altitude moy.", value: s.mean?.toFixed(0),      unit: "m", accent: true },
      { label: "Altitude min.", value: s.min?.toFixed(0),       unit: "m" },
      { label: "Altitude max.", value: s.max?.toFixed(0),       unit: "m" },
      { label: "Dénivelé",      value: s.amplitude?.toFixed(0), unit: "m" },
    ],
    distTitle: "Distribution des altitudes",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par tranche altitudinale",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · SRTM 30m · Statique",
  },

  "Pente": {
    icon: "📐", label: "Pente SRTM",
    gradient: ["#ffffff","#fdae61","#d73027"],
    gradientLabels: ["0° (plat)", "30°", "60°+ (falaise)"],
    statCards: (s) => [
      { label: "Pente moyenne", value: s.mean?.toFixed(1),       unit: "°", accent: true },
      { label: "Pente max",     value: s.max?.toFixed(1),        unit: "°" },
      { label: "Zone plate",    value: s.flat_pct?.toFixed(0),   unit: "%" },
      { label: "Zone pentue",   value: s.steep_pct?.toFixed(0),  unit: "%" },
    ],
    distTitle: "Distribution des pentes",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe de pente",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · SRTM 30m · Statique",
  },

  "Ombrage": {
    icon: "🌓", label: "Ombrage SRTM",
    gradient: ["#000000","#888888","#ffffff"],
    gradientLabels: ["Ombre totale", "Mi-ombre", "Plein soleil"],
    statCards: (s) => [
      { label: "Ombrage moyen", value: s.mean?.toFixed(0),         unit: "",  accent: true },
      { label: "Zone ombragée", value: s.shadow_pct?.toFixed(0),   unit: "%"  },
      { label: "Zone exposée",  value: s.exposed_pct?.toFixed(0),  unit: "%"  },
      { label: "Contraste",     value: s.std?.toFixed(0),           unit: "σ"  },
    ],
    distTitle: "Distribution de l'ombrage",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par exposition",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · SRTM 30m · Statique",
  },

  // ── Canopée WRI/Meta ──────────────────────────────────────
  "Hauteur canopée": {
    icon: "🌲", label: "Hauteur de canopée",
    gradient: ["#ffffff","#d9f0a3","#addd8e","#78c679","#41ab5d","#238443","#005a32"],
    gradientLabels: ["0 m (sol)", "15 m", "30+ m"],
    statCards: (s) => [
      { label: "Hauteur moy.",   value: s.mean?.toFixed(1),     unit: "m", accent: true },
      { label: "Hauteur max.",   value: s.max?.toFixed(0),      unit: "m" },
      { label: "Couverture",     value: s.cover_pct?.toFixed(0), unit: "%" },
      { label: "Surface boisée", value: s.cover_km2 >= 1 ? s.cover_km2?.toFixed(1) : (s.cover_km2 * 100)?.toFixed(0), unit: s.cover_km2 >= 1 ? "km²" : "ha" },
    ],
    distTitle: "Distribution des hauteurs de canopée",
    distClasses: (s) => s.classes || [],
    surfaceTitle: "Surfaces par classe de hauteur",
    surfaceClasses: (s) => s.surfaces || [],
    infoExtra: () => " · WRI/Meta 2020 · ~1 m",
  },
};

const GENERIC_CONFIG = {
  icon: "📊", label: "Statistiques",
  gradient: ["#440154","#31688e","#35b779","#fde725"],
  gradientLabels: ["Min", "Médiane", "Max"],
  statCards: (s) => [
    { label: "Moyenne", value: s.mean?.toFixed(2), unit: "", accent: true },
    { label: "Min",     value: s.min?.toFixed(2),  unit: "" },
    { label: "Max",     value: s.max?.toFixed(2),  unit: "" },
    { label: "Pixels",  value: s.valid_pixels?.toLocaleString(), unit: "" },
  ],
  distTitle: "Distribution des valeurs",
  distClasses: (s) => s.classes || [],
  surfaceTitle: "Surfaces par classe",
  surfaceClasses: (s) => s.surfaces || [],
  infoExtra: () => "",
};

const SOURCE_LABELS = {
  sentinel2: "Sentinel-2", landsat9: "Landsat 9", landsat8: "Landsat 8",
  modis_ndvi: "MODIS 500m", modis_lst: "MODIS 1km", era5: "ERA5 11km",
  sentinel1: "S-1 SAR", worldcover: "ESA WorldCover 10m",
  hansen: "Hansen GFW 30m", srtm: "SRTM 30m", canopy_height: "WRI/Meta ~1m",
};

// ══════════════════════════════════════════════════════════════
// SOUS-COMPOSANTS
// ══════════════════════════════════════════════════════════════

function StatCard({ label, value, unit, accent, accentRed, C }) {
  const color = accentRed ? C.red : accent ? C.acc : C.txt;
  return (
    <div style={{ background: C.hover, borderRadius: 6, padding: "8px 10px", border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 2 }}>
      <span style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>{label}</span>
      <span style={{ fontFamily: M, fontSize: 17, fontWeight: 500, color, lineHeight: 1 }}>
        {value ?? "–"}<span style={{ fontSize: 10, fontWeight: 400, color: C.dim, marginLeft: 3 }}>{unit}</span>
      </span>
    </div>
  );
}

function DistBar({ label, pct, color, surface, C }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 5 }}>
      <span style={{ fontFamily: M, fontSize: 10, color: C.dim, width: 100, flexShrink: 0, textAlign: "right" }}>{label}</span>
      <div style={{ flex: 1, height: 7, background: C.hover, borderRadius: 4, overflow: "hidden" }}>
        <div style={{ width: `${Math.max(pct, 1)}%`, height: "100%", background: color, borderRadius: 4, transition: "width .5s ease" }} />
      </div>
      <span style={{ fontFamily: M, fontSize: 10, color: C.dim, width: 30, textAlign: "right", flexShrink: 0 }}>{pct?.toFixed(0)}%</span>
      <span style={{ fontFamily: M, fontSize: 10, color: C.dim, width: 56, textAlign: "right", flexShrink: 0 }}>{surface}</span>
    </div>
  );
}

function SurfaceCard({ dot, name, km2, ha, C }) {
  const display = km2 >= 1 ? `${km2.toFixed(2)} km²` : `${(km2 * 100).toFixed(0)} ha`;
  return (
    <div style={{ background: C.hover, borderRadius: 6, padding: "7px 9px", border: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", gap: 7 }}>
      <div style={{ width: 8, height: 8, borderRadius: "50%", background: dot, flexShrink: 0 }} />
      <div style={{ flex: 1, minWidth: 0 }}>
        <div style={{ fontSize: 9, color: C.dim }}>{name}</div>
        <div style={{ fontFamily: M, fontSize: 12, fontWeight: 500, color: C.txt }}>
          {display}<span style={{ fontFamily: F, fontSize: 9, color: C.dim, fontWeight: 400, marginLeft: 3 }}>· {ha?.toFixed(0)} ha</span>
        </div>
      </div>
    </div>
  );
}

function ColorLegend({ classes, C }) {
  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 5, marginBottom: 2 }}>
      {classes.map((c, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: 3 }}>
          <div style={{ width: 8, height: 8, borderRadius: 2, background: c.color, flexShrink: 0 }} />
          <span style={{ fontSize: 9, color: C.dim }}>{c.label}</span>
        </div>
      ))}
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// COMPOSANT PRINCIPAL
// ══════════════════════════════════════════════════════════════
export default function IndexStatsModal({ dataset, index, layer, bbox, roiGeoJSON, geeParams, onClose }) {
  const C = useThemeContext();
  const [stats,   setStats]   = useState(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(null);

  const cfg      = INDEX_CONFIG[index] || GENERIC_CONFIG;
  const panelRef = useRef(null);
  const dragRef  = useRef({ ox: 0, oy: 0 });
  const [pos, setPos] = useState(null);
  const [z, setZ] = useState(() => nextZ());   // profondeur partagée (clic = premier plan)
  const [isMobile, setIsMobile] = useState(() => typeof window !== "undefined" && window.innerWidth < 640);

  useEffect(() => {
    setPos({ x: Math.max(20, window.innerWidth - 480), y: 80 });
    const onR = () => setIsMobile(window.innerWidth < 640);
    window.addEventListener("resize", onR);
    return () => window.removeEventListener("resize", onR);
  }, []);

  const onDragStart = useCallback((e) => {
    if (e.button !== 0) return;
    e.preventDefault();
    const rect = panelRef.current?.getBoundingClientRect();
    dragRef.current = { ox: e.clientX - rect.left, oy: e.clientY - rect.top };
    const onMove = (ev) => setPos({
      x: Math.max(0, Math.min(window.innerWidth  - 460, ev.clientX - dragRef.current.ox)),
      y: Math.max(0, Math.min(window.innerHeight - 80,  ev.clientY - dragRef.current.oy)),
    });
    const onUp = () => { window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);

  useEffect(() => {
    const h = e => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [onClose]);

  const fetchStats = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const body = { dataset, index };
      if (roiGeoJSON) body.roi_geojson = roiGeoJSON;
      else if (bbox)  body.bbox = bbox;
      if (geeParams) {
        body.date_start = geeParams.date_start;
        body.date_end   = geeParams.date_end;
        body.cloud_max  = geeParams.cloud_max;
        body.composite  = geeParams.composite;
      }
      const res  = await fetch(`${API}/api/gee/index/stats`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);
      setStats(data);
    } catch (e) { setError(e.message); }
    setLoading(false);
  }, [dataset, index, bbox, roiGeoJSON, geeParams]);

  useEffect(() => { fetchStats(); }, [fetchStats]);

  const exportCSV = () => {
    if (!stats) return;
    const rows = [
      ["Dataset", dataset], ["Index", index], ["", ""],
      ...cfg.statCards(stats).map(c => [c.label, c.value, c.unit]),
      ["", ""], ["Classe", "Pct %", "km²", "ha"],
      ...cfg.distClasses(stats).map(d => [d.label, d.pct?.toFixed(1), d.km2?.toFixed(3), d.ha?.toFixed(1)]),
      ...(cfg.dist2Classes ? [["", ""], ...(cfg.dist2Classes(stats).map(d => [d.label, d.pct?.toFixed(1), d.km2?.toFixed(3), d.ha?.toFixed(1)]))] : []),
      ["", ""], ["Catégorie", "km²", "ha"],
      ...cfg.surfaceClasses(stats).map(s => [s.name, s.km2?.toFixed(3), s.ha?.toFixed(1)]),
    ];
    const csv  = rows.map(r => r.join(";")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a");
    a.href = url; a.download = `${dataset}_${index}_stats.csv`.replace(/[\s/]/g, "_"); a.click();
    URL.revokeObjectURL(url);
  };

  const subtitle = [
    geeParams?.date_start,
    geeParams?.composite ? `Composite ${geeParams.composite}` : null,
    "Zone affichée",
  ].filter(Boolean).join(" · ");

  if (!pos) return null;

  const distClasses  = cfg.distClasses(stats || {});
  const dist2Classes = cfg.dist2Classes ? cfg.dist2Classes(stats || {}) : [];
  const surfClasses  = cfg.surfaceClasses(stats || {});
  const sourceLabel  = SOURCE_LABELS[dataset] || dataset;

  const frame = isMobile
    ? { position: "fixed", left: 8, right: 8, bottom: 8, top: "auto", width: "auto", maxHeight: "82vh" }
    : { position: "fixed", left: pos.x, top: pos.y, width: 460, maxWidth: "96vw", maxHeight: "calc(100vh - 100px)" };

  return (
    <div ref={panelRef} onMouseDown={() => setZ(bumpZ)} style={{
      ...frame, zIndex: z,
      background: C.card, border: `0.5px solid ${C.bdr}`,
      borderRadius: 10, boxShadow: "0 4px 24px rgba(0,0,0,0.30)",
      display: "flex", flexDirection: "column", overflow: "hidden", userSelect: "none",
    }}>

      {/* En-tête draggable (desktop) */}
      <div onMouseDown={isMobile ? undefined : onDragStart} style={{
        padding: "10px 14px", borderBottom: `0.5px solid ${C.bdr}`,
        display: "flex", alignItems: "center", justifyContent: "space-between",
        cursor: "grab", flexShrink: 0,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ display: "flex", color: C.dim }}><IcMove size={13}/></span>
          <div>
            <div style={{ fontSize: 12, fontWeight: 600, color: C.txt, display: "flex", alignItems: "center", gap: 6 }}>
              Statistiques {cfg.label}
              <span style={{ fontSize: 9, padding: "1px 6px", borderRadius: 3, background: C.hover, border: `0.5px solid ${C.bdr}`, color: C.dim, fontWeight: 400 }}>
                {sourceLabel}
              </span>
            </div>
            <div style={{ fontSize: 9, color: C.dim, marginTop: 1 }}>{subtitle}</div>
          </div>
        </div>
        <button onClick={onClose} style={{ background: "none", border: "none", cursor: "pointer", color: C.dim, display: "flex", padding: 2 }}><IcX size={16}/></button>
      </div>

      {/* Corps */}
      <div style={{ flex: 1, overflowY: "auto", padding: "12px 14px", display: "flex", flexDirection: "column", gap: 12 }}>

        {loading && (
          <div style={{ textAlign: "center", padding: "24px 0", color: C.dim, fontSize: 12 }}>
            <div style={{ display: "flex", justifyContent: "center", marginBottom: 6 }}><IcLoader size={18}/></div>
            Calcul en cours via Google Earth Engine…
          </div>
        )}

        {error && !loading && (
          <div style={{ padding: "8px 10px", borderRadius: 6, fontSize: 11, background: C.red + "15", color: C.red, border: `0.5px solid ${C.red}44` }}>
            {error}
            <button onClick={fetchStats} style={{ marginLeft: 8, fontSize: 10, color: C.acc, background: "none", border: "none", cursor: "pointer", textDecoration: "underline" }}>Réessayer</button>
          </div>
        )}

        {stats && !loading && (<>

          {/* Stat cards */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0,1fr))", gap: 6 }}>
            {cfg.statCards(stats).map((sc, i) => (
              <StatCard key={i} label={sc.label} value={sc.value} unit={sc.unit} accent={sc.accent} accentRed={sc.accentRed} C={C} />
            ))}
          </div>

          {/* Gradient ou légende couleurs */}
          {!cfg.noGradient && cfg.gradient.length > 0 && (
            <div>
              <div style={{ height: 7, borderRadius: 4, background: `linear-gradient(to right, ${cfg.gradient.join(", ")})` }} />
              <div style={{ display: "flex", justifyContent: "space-between", marginTop: 3 }}>
                {cfg.gradientLabels.map((l, i) => <span key={i} style={{ fontSize: 9, color: C.dim }}>{l}</span>)}
              </div>
            </div>
          )}
          {cfg.noGradient && distClasses.length > 0 && (
            <ColorLegend classes={distClasses} C={C} />
          )}

          {/* Distribution principale */}
          {distClasses.length > 0 && (
            <div>
              <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 7 }}>{cfg.distTitle}</div>
              {distClasses.map((c, i) => (
                <DistBar key={i} label={c.label} pct={c.pct} color={c.color}
                  surface={c.km2 >= 1 ? `${c.km2.toFixed(2)} km²` : `${(c.km2 * 100).toFixed(0)} ha`} C={C} />
              ))}
            </div>
          )}

          {/* Distribution secondaire (Hansen décennies) */}
          {dist2Classes.length > 0 && (
            <div>
              <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 7 }}>{cfg.dist2Title}</div>
              {dist2Classes.map((c, i) => (
                <DistBar key={i} label={c.label} pct={c.pct} color={c.color}
                  surface={c.km2 >= 1 ? `${c.km2.toFixed(2)} km²` : `${(c.km2 * 100).toFixed(0)} ha`} C={C} />
              ))}
            </div>
          )}

          {/* Surfaces */}
          {surfClasses.length > 0 && (
            <div>
              <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 7 }}>{cfg.surfaceTitle}</div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0,1fr))", gap: 5 }}>
                {surfClasses.map((s, i) => (
                  <SurfaceCard key={i} dot={s.color} name={s.name} km2={s.km2} ha={s.ha} C={C} />
                ))}
              </div>
            </div>
          )}

          {/* Barre info */}
          <div style={{ fontSize: 9, color: C.dim, borderTop: `0.5px solid ${C.bdr}`, paddingTop: 8 }}>
            Zone : <span style={{ fontFamily: M, color: C.txt }}>{stats.total_km2?.toFixed(2)} km²</span>
            {" · "}Pixels : <span style={{ fontFamily: M, color: C.txt }}>{stats.valid_pixels?.toLocaleString()}</span>
            {" · "}Résol. : <span style={{ fontFamily: M, color: C.txt }}>{stats.scale} m</span>
            {cfg.infoExtra(stats)}
          </div>

        </>)}
      </div>

      {/* Pied */}
      <div style={{ padding: "8px 14px", borderTop: `0.5px solid ${C.bdr}`, display: "flex", gap: 6, justifyContent: "flex-end", flexShrink: 0 }}>
        {stats && (
          <button onClick={exportCSV} style={{ fontFamily: F, fontSize: 10, padding: "5px 10px", borderRadius: 5, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim, cursor: "pointer" }}>
            ↓ CSV
          </button>
        )}
        <button onClick={onClose} style={{ fontFamily: F, fontSize: 10, fontWeight: 600, padding: "5px 12px", borderRadius: 5, background: C.acc, border: "none", color: "#fff", cursor: "pointer" }}>
          Fermer
        </button>
      </div>
    </div>
  );
}
