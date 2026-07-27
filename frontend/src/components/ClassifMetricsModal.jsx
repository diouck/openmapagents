/**
 * ClassifMetricsModal.jsx — Résultats de classification
 *
 * 3 modes d'affichage selon result.backend :
 *   • "gee" / "sklearn"  → métriques supervisées complètes (3 tabs)
 *   • "gee_auto"         → données pré-classifiées (légende + infos)
 *   • "gee_cluster"      → clustering non supervisé (clusters + légende)
 */
import { useState } from "react";
import { useThemeContext } from "../theme";
import { F } from "../config";
import { IcSun, IcCloud, IcCloudFog, IcGlobe, IcSatellite, IcTreePine, IcShuffle,
  IcRocket, IcScissors, IcMapPin, IcBarChart, IcBulb, IcInfo, IcCircleDot,
  IcMicroscope, IcX, IcTrendingUp, IcClipboard, IcGrid3, IcHash, IcCalendar,
  IcAlert, IcCheck, IcSettings } from "../icons";

// ── Formatage surface ──────────────────────────────────────────────────────────
function fmtArea(ha) {
  if (ha === null || ha === undefined || ha === 0) return null;
  if (ha < 1)   return `${Math.round(ha * 10000)} m²`;
  if (ha < 100) return `${ha.toFixed(1)} ha`;
  return `${(ha / 100).toFixed(2)} km²`;
}

// ── Donut Pie Chart ────────────────────────────────────────────────────────────
function PieChart({ legend, C }) {
  const entries = (legend || []).filter(e => e.area_ha > 0);
  const total   = entries.reduce((s, e) => s + (e.area_ha || 0), 0);
  if (!total || entries.length < 2) return null;

  const R = 54, IR = 26, CX = 70, CY = 70;
  let angle = -Math.PI / 2;

  const slices = entries.map(e => {
    const pct = e.area_ha / total;
    const a0  = angle;
    angle    += pct * 2 * Math.PI;
    return { ...e, pct, a0, a1: angle };
  });

  const arcPath = (a0, a1) => {
    // Évite le path "cercle complet" qui ne s'affiche pas
    const safeA1 = a1 - a0 >= 2 * Math.PI ? a0 + 2 * Math.PI - 0.0001 : a1;
    const large  = safeA1 - a0 > Math.PI ? 1 : 0;
    const ox = CX + R  * Math.cos(a0), oy = CY + R  * Math.sin(a0);
    const ex = CX + R  * Math.cos(safeA1), ey = CY + R  * Math.sin(safeA1);
    const ix = CX + IR * Math.cos(safeA1), iy = CY + IR * Math.sin(safeA1);
    const jx = CX + IR * Math.cos(a0),  jy = CY + IR * Math.sin(a0);
    return `M${ox},${oy} A${R},${R} 0 ${large} 1 ${ex},${ey} L${ix},${iy} A${IR},${IR} 0 ${large} 0 ${jx},${jy} Z`;
  };

  const totalKm2 = (total / 100).toFixed(2);

  return (
    <div style={{ display:"flex", gap:16, alignItems:"center", flexWrap:"wrap" }}>
      {/* Donut */}
      <svg width={140} height={140} style={{ flexShrink:0 }}>
        {slices.map(s => (
          <path key={s.class_id} d={arcPath(s.a0, s.a1)}
                fill={s.color} stroke={C.bg} strokeWidth={1.5} />
        ))}
        <text x={CX} y={CY-6}  textAnchor="middle" fontSize={9}  fill={C.dim}>Total</text>
        <text x={CX} y={CY+8}  textAnchor="middle" fontSize={11} fontWeight={700} fill={C.txt}>
          {totalKm2} km²
        </text>
      </svg>

      {/* Légende % + km² */}
      <div style={{ display:"flex", flexDirection:"column", gap:5, flex:1, minWidth:160 }}>
        {slices.map(s => {
          const km2 = (s.area_ha / 100).toFixed(s.area_ha >= 10 ? 2 : 3);
          const pct = Math.round(s.pct * 100);
          return (
            <div key={s.class_id} style={{ display:"flex", alignItems:"center", gap:6 }}>
              <div style={{ width:9, height:9, borderRadius:2, background:s.color, flexShrink:0 }}/>
              <span style={{ fontSize:10, color:C.txt, flex:1, overflow:"hidden",
                             textOverflow:"ellipsis", whiteSpace:"nowrap" }}>
                {s.label}
              </span>
              <span style={{ fontSize:10, fontWeight:700, color:s.color,
                             width:34, textAlign:"right" }}>
                {pct}%
              </span>
              <span style={{ fontSize:9, color:C.dim, fontFamily:"monospace",
                             width:60, textAlign:"right" }}>
                {s.area_ha >= 100 ? `${km2} km²` : `${s.area_ha.toFixed(1)} ha`}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Badge couverture nuageuse ───────────────────────────────────────────────────
function CloudBadge({ cloudPct, imageCount, C }) {
  if (cloudPct === null || cloudPct === undefined) return null;
  const good = cloudPct < 10;
  const ok   = cloudPct < 25;
  const color = good ? "#4daf4a" : ok ? "#e07b00" : "#e41a1c";
  const Icon  = good ? IcSun : ok ? IcCloud : IcCloudFog;
  return (
    <div style={{
      display:"flex", alignItems:"center", gap:10,
      padding:"8px 12px", borderRadius:8,
      background: color + "12", border:`0.5px solid ${color}44`,
    }}>
      <Icon size={18} color={color}/>
      <div>
        <div style={{ fontSize:11, fontWeight:600, color }}>
          {cloudPct.toFixed(1)}% de couverture nuageuse moyenne
        </div>
        {imageCount > 0 && (
          <div style={{ fontSize:10, color:C.dim, marginTop:1 }}>
            {imageCount} image{imageCount > 1 ? "s" : ""} composite dans la période
            {good ? " — qualité excellente" : ok ? " — qualité correcte" : " — qualité limitée, envisagez une autre période"}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Helpers couleur ────────────────────────────────────────────────────────────
function lerp(a, b, t) { return a + (b-a)*t; }
function blueScale(t) {
  return `rgb(${Math.round(lerp(240,25,t))},${Math.round(lerp(248,100,t))},${Math.round(lerp(255,200,t))})`;
}
function redScale(t) {
  return `rgb(${Math.round(lerp(255,180,t))},${Math.round(lerp(248,20,t))},${Math.round(lerp(240,20,t))})`;
}

// ── Confusion Matrix SVG ───────────────────────────────────────────────────────
function ConfusionMatrix({ matrix, labels, C }) {
  const n = labels.length;
  if (!n || !matrix?.length) return (
    <p style={{ color:C.dim, fontSize:11, textAlign:"center" }}>Matrice non disponible</p>
  );
  const CELL=56, LEFT=100, TOP=60;
  const W=LEFT+n*CELL+20, H=TOP+n*CELL+40;
  const maxVal=Math.max(...matrix.flat().map(Number))||1;
  return (
    <div style={{ overflowX:"auto" }}>
      <svg width={W} height={H} style={{ fontFamily:F }}>
        <text x={10} y={TOP+n*CELL/2} textAnchor="middle" fontSize={10} fill={C.dim}
              transform={`rotate(-90,10,${TOP+n*CELL/2})`}>Réel</text>
        <text x={LEFT+n*CELL/2} y={H-4} textAnchor="middle" fontSize={10} fill={C.dim}>Prédit</text>
        {labels.map((lbl,i)=>(
          <g key={`r${i}`}>
            <text x={LEFT-8} y={TOP+i*CELL+CELL/2+4} textAnchor="end" fontSize={10} fill={C.txt}>
              {lbl.length>12?lbl.slice(0,11)+"…":lbl}
            </text>
            {labels.map((_,j)=>{
              const val=Number(matrix[i]?.[j]??0);
              const t=maxVal>0?val/maxVal:0;
              const bg=i===j?blueScale(t):(val>0?redScale(t*0.7):"#f8f9fa");
              const fg=t>0.5?"#fff":C.txt;
              return (
                <g key={`c${i}${j}`}>
                  <rect x={LEFT+j*CELL} y={TOP+i*CELL} width={CELL-2} height={CELL-2} rx={4}
                        fill={bg} stroke={C.bdr} strokeWidth={0.5}/>
                  <text x={LEFT+j*CELL+CELL/2} y={TOP+i*CELL+CELL/2+4} textAnchor="middle"
                        fontSize={11} fontWeight={i===j?600:400} fill={fg}>{val}</text>
                </g>
              );
            })}
          </g>
        ))}
        {labels.map((lbl,j)=>(
          <text key={`h${j}`} x={LEFT+j*CELL+CELL/2} y={TOP-8} textAnchor="end"
                fontSize={10} fill={C.txt}
                transform={`rotate(-35,${LEFT+j*CELL+CELL/2},${TOP-8})`}>
            {lbl.length>12?lbl.slice(0,11)+"…":lbl}
          </text>
        ))}
        <rect x={LEFT} y={H-13} width={9} height={9} rx={2} fill={blueScale(0.85)}/>
        <text x={LEFT+13} y={H-4} fontSize={9} fill={C.dim}>Diagonale = bonnes classifications</text>
        <rect x={LEFT+188} y={H-13} width={9} height={9} rx={2} fill={redScale(0.55)}/>
        <text x={LEFT+201} y={H-4} fontSize={9} fill={C.dim}>Hors diagonale = erreurs</text>
      </svg>
    </div>
  );
}

// ── Feature Importance ─────────────────────────────────────────────────────────
function FeatureImportance({ data, C }) {
  if (!data||!Object.keys(data).length) return (
    <p style={{ color:C.dim, fontSize:11, textAlign:"center", padding:20 }}>
      Importance non disponible pour ce modèle (SVM, Min. Distance, Naïve Bayes)
    </p>
  );
  const items=Object.entries(data).sort((a,b)=>b[1]-a[1]).slice(0,20);
  const max=items[0]?.[1]||1;
  return (
    <div style={{ display:"flex", flexDirection:"column", gap:6 }}>
      {items.map(([name,val])=>(
        <div key={name} style={{ display:"flex", alignItems:"center", gap:8 }}>
          <span style={{ fontSize:11, color:C.txt, width:80, flexShrink:0,
                         overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>
            {name}
          </span>
          <div style={{ flex:1, height:18, background:C.hover, borderRadius:4, overflow:"hidden" }}>
            <div style={{ width:`${(val/max)*100}%`, height:"100%",
                          background:"#4A90D9", borderRadius:4, transition:"width .3s" }}/>
          </div>
          <span style={{ fontSize:10, color:C.dim, width:44, textAlign:"right" }}>
            {(val*100).toFixed(1)}%
          </span>
        </div>
      ))}
    </div>
  );
}

// ── Badge coloré accuracy ──────────────────────────────────────────────────────
function AccBadge({ value, label }) {
  const pct=Math.round((value||0)*100);
  const bg=pct>=80?"#4daf4a22":pct>=60?"#ff7f0022":"#e41a1c22";
  const fg=pct>=80?"#4daf4a"  :pct>=60?"#e07b00"  :"#e41a1c";
  return (
    <div style={{ background:bg, color:fg, borderRadius:8, padding:"4px 12px",
                  fontSize:22, fontWeight:700, textAlign:"center" }}>
      {pct}%
      <div style={{ fontSize:10, fontWeight:400, marginTop:2 }}>{label}</div>
    </div>
  );
}

// ── Légende des classes (partagée) ────────────────────────────────────────────
function LegendBadges({ legend, C }) {
  if (!legend?.length) return null;
  return (
    <div style={{ display:"flex", flexWrap:"wrap", gap:8 }}>
      {legend.map(l => (
        <div key={l.class_id} style={{
          display:"flex", alignItems:"center", gap:6,
          padding:"3px 10px", borderRadius:20,
          border:`1px solid ${C.bdr}`, fontSize:11, color:C.txt,
        }}>
          <span style={{ width:10, height:10, borderRadius:"50%",
                         background:l.color, flexShrink:0 }}/>
          {l.label}
        </div>
      ))}
    </div>
  );
}

// ── Base de connaissances produits GEE ─────────────────────────────────────────
const PRODUCT_KB = {
  dynamicworld: {
    icon: IcGlobe,
    name: "Dynamic World (Google)",
    resolution: "10 m · Sentinel-2 · 9 classes",
    description:
      "Classification pixel par pixel en temps quasi-réel par réseau de neurones (DNN). " +
      "Chaque image Sentinel-2 (~5 jours de revisite) est classifiée indépendamment.",
    limits: [
      "Sensible aux nuages résiduels, ombres et variations phénologiques (pas de composite)",
      "Classes 'probabilistes' : un même pixel peut changer de classe entre deux dates",
      "Moins précis que ESA WorldCover sur certaines régions (pas de validation terrain systématique)",
      "Biais régionaux marqués sur zones tropicales et arides",
    ],
    opportunities: [
      "Quasi-temps réel (≈ 5 jours de délai) — idéal pour suivi de changement rapide",
      "Archive continue depuis 2015 — séries temporelles longues",
      "9 classes universelles (eau, forêt, cultures, bâti, sol nu…)",
      "Probabilité par classe accessible : permet des analyses de transition",
    ],
    note: "Pour comparer 2022 vs 2023, utilisez le mode multi-dates dans l'onglet Auto GEE.",
  },
  worldcover: {
    icon: IcGlobe,
    name: "ESA WorldCover",
    resolution: "10 m · Sentinel-1 + Sentinel-2 · 11 classes",
    description:
      "Carte d'occupation du sol annuelle produite par l'ESA. " +
      "Composite annuel (meilleure observation par pixel sur toute l'année) validé par campagne terrain mondiale.",
    limits: [
      "Statique (cartes 2020 et 2021 uniquement) — pas de suivi dynamique",
      "11 classes fixes, pas personnalisables selon le contexte local",
      "Cohérence temporelle limitée (seulement 2 millésimes disponibles)",
    ],
    opportunities: [
      "Composite annuel = très stable, quasi insensible aux nuages et phénologies",
      "Validation terrain rigoureuse : >75 % précision globale (200 000 points terrain)",
      "Fusion Sentinel-1 (radar) + Sentinel-2 (optique) → robuste aux nuages",
      "Cohérence spatiale appliquée (lissage thématique, pas de 'sel et poivre')",
    ],
    note: "Référence cartographique mondiale — idéale pour un état de l'occupation du sol de référence.",
  },
  modis: {
    icon: IcSatellite,
    name: "MODIS (NASA)",
    resolution: "500 m · Terra/Aqua · 17 classes IGBP",
    description:
      "Produit de classification annuelle MODIS basé sur 17 classes de couverture du sol (IGBP). " +
      "Composite multi-observations annuel sur 500 m.",
    limits: [
      "Résolution spatiale grossière (500 m) — inadapté sous 1 km²",
      "Classes IGBP standardisées, pas toujours adaptées au contexte local ou agri",
      "Confusions fréquentes entre cultures et prairies dans les zones tempérées",
    ],
    opportunities: [
      "Archive complète depuis 2001 — excellent pour tendances pluri-décennales",
      "Composition robuste (meilleure observation par pixel)",
      "Haute fréquence temporelle (16 jours) disponible pour analyses dynamiques",
      "Libre accès sans quota GEE significatif",
    ],
    note: "Recommandé pour les analyses régionales ou à l'échelle continentale.",
  },
  copernicus: {
    icon: IcGlobe,
    name: "Copernicus Global Land",
    resolution: "100 m · Multi-capteurs · 23 classes",
    description:
      "Service Global Land Copernicus de l'ESA, produit annuel à 100 m incluant " +
      "des couches de fraction de végétation, d'eau et de sol nu en plus des classes thématiques.",
    limits: [
      "Résolution 100 m — moins précis que Sentinel pour les détails fins",
      "Millésimes limités (2015–2019 selon le produit)",
      "Moins adapté aux zones forestières denses (confusion forêt/ombre)",
    ],
    opportunities: [
      "Inclut des couches de fraction (FCover, FAPAr, LAI) en plus des classes",
      "Calibration radiométrique rigoureuse par ESA",
      "Compatible avec les nomenclatures européennes (Corine Land Cover)",
    ],
    note: "Bon compromis régional pour l'Europe et l'Afrique subsaharienne.",
  },
};

function getProductKB(classifier_label) {
  if (!classifier_label) return null;
  const lbl = classifier_label.toLowerCase();
  if (lbl.includes("dynamic")) return PRODUCT_KB.dynamicworld;
  if (lbl.includes("worldcover") || lbl.includes("world cover") || lbl.includes("esa")) return PRODUCT_KB.worldcover;
  if (lbl.includes("modis"))     return PRODUCT_KB.modis;
  if (lbl.includes("copernicus")) return PRODUCT_KB.copernicus;
  return null;
}

// ── Base de connaissances modèles supervisés ──────────────────────────────────
const MODEL_KB = {
  smileRandomForest: {
    icon: IcTreePine,
    name: "Random Forest",
    description: "Ensemble d'arbres de décision entraînés sur des sous-échantillons aléatoires (bagging). " +
      "Très robuste au sur-apprentissage, gère bien les valeurs aberrantes.",
    limits: [
      "Moins interprétable qu'un arbre unique (boîte noire partielle)",
      "Lent sur très grands ROIs si numberOfTrees élevé",
      "Peut sous-performer avec des données déséquilibrées (classes rares)",
    ],
    opportunities: [
      "Excellent rapport précision/robustesse — recommandé par défaut",
      "L'importance des features est disponible nativement",
      "Insensible aux valeurs aberrantes et bruit spectral modéré",
    ],
    tip: "Augmentez 'Nb arbres' (≥ 200) pour améliorer la stabilité sur des AOI larges.",
  },
  smileCart: {
    icon: IcShuffle,
    name: "Arbre de décision (CART)",
    description: "Arbre binaire récursif qui partitionne l'espace spectral. " +
      "Très interprétable mais sensible au sur-apprentissage.",
    limits: [
      "Sur-apprentissage fort sur les données d'entraînement (accuracy sur train ≠ terrain)",
      "Frontières de décision en escaliers — peu adaptées aux gradients spectraux",
      "Instable : un changement de ROI peut modifier fortement l'arbre",
    ],
    opportunities: [
      "Totalement interprétable (règles SI/ALORS lisibles)",
      "Très rapide à entraîner et à appliquer",
      "Bon outil pédagogique pour comprendre la classification spectrale",
    ],
    tip: "Limitez 'Max nœuds' pour éviter le sur-apprentissage (ex. 50–100).",
  },
  smileGradientTreeBoost: {
    icon: IcRocket,
    name: "Gradient Boosting",
    description: "Ensemble d'arbres construits séquentiellement, chaque arbre corrigeant les erreurs du précédent. " +
      "Offre souvent la meilleure précision mais plus sensible aux hyperparamètres.",
    limits: [
      "Risque de sur-apprentissage si learning rate élevé ou trop d'estimateurs",
      "Entraînement plus long que Random Forest",
      "Peu robuste au bruit dans les ROIs (outliers spectraux)",
    ],
    opportunities: [
      "Souvent la meilleure accuracy sur des datasets bien labellisés",
      "Importance des features disponible",
      "Adapté aux classes spectralement proches (grandes cultures vs prairies)",
    ],
    tip: "Utilisez un learning rate faible (0.01–0.05) avec ≥ 200 estimateurs pour la stabilité.",
  },
  libsvm: {
    icon: IcScissors,
    name: "SVM (Support Vector Machine)",
    description: "Classifieur à marge maximale dans un espace transformé par kernel. " +
      "Efficace sur les petits datasets avec peu de classes.",
    limits: [
      "Peu adapté aux grands datasets (O(n²) en mémoire)",
      "Sensible au choix du kernel et du paramètre C",
      "Pas d'importance des features disponible nativement dans GEE",
    ],
    opportunities: [
      "Très efficace avec peu d'échantillons d'entraînement",
      "Robuste en haute dimension (nombreuses bandes)",
      "Kernel RBF → frontières de décision non linéaires",
    ],
    tip: "Idéal pour ≤ 5 000 pixels d'entraînement et ≥ 6 bandes spectrales.",
  },
  minimumDistance: {
    icon: IcMapPin,
    name: "Distance minimale (k-NN GEE)",
    description: "Assigne chaque pixel à la classe dont le centroïde est le plus proche en distance euclidienne " +
      "(ou cosinus/manhattan). Très simple et rapide.",
    limits: [
      "Très sensible à la qualité et représentativité des ROIs",
      "Aucune frontière de décision apprise — ignores la variance intra-classe",
      "Performances faibles sur des classes spectralement proches",
    ],
    opportunities: [
      "Extrêmement rapide à entraîner et appliquer",
      "Interprétable : distance au centre de masse de chaque classe",
      "Utile comme baseline ou pour des classifications rapides",
    ],
    tip: "Recommandé uniquement si vos classes sont spectralement très distinctes.",
  },
  smileNaiveBayes: {
    icon: IcBarChart,
    name: "Naïve Bayes",
    description: "Classifieur probabiliste basé sur le théorème de Bayes avec hypothèse d'indépendance " +
      "conditionnelle des features. Rapide et peu gourmand en mémoire.",
    limits: [
      "Hypothèse d'indépendance des bandes rarement vraie (corrélation NIR/SWIR)",
      "Moins précis que RF ou Gradient Boost sur données corrélées",
      "Sensible aux distributions non gaussiennes",
    ],
    opportunities: [
      "Entraînement quasi-instantané",
      "Fonctionne bien sur les petits datasets",
      "Produit des probabilités par classe exploitables",
    ],
    tip: "Bon choix pour une classification rapide exploratoire ou avec très peu de ROIs.",
  },
};

// ── Carte produit / modèle (limites + opportunités) ─────────────────────────
function KnowledgeCard({ kb, C }) {
  if (!kb) return null;
  return (
    <div style={{
      borderRadius: 10, border: `1px solid ${C.bdr}`,
      overflow: "hidden", fontSize: 10,
    }}>
      {/* Header */}
      <div style={{
        padding: "10px 14px",
        background: C.hover,
        borderBottom: `0.5px solid ${C.bdr}`,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
          <span style={{ display: "flex", color: C.acc }}>{kb.icon && <kb.icon size={16}/>}</span>
          <div>
            <div style={{ fontSize: 12, fontWeight: 700, color: C.txt }}>{kb.name}</div>
            {kb.resolution && (
              <div style={{ fontSize: 9, color: C.dim, marginTop: 1 }}>{kb.resolution}</div>
            )}
          </div>
        </div>
        <div style={{ color: C.dim, lineHeight: 1.5 }}>{kb.description}</div>
      </div>

      {/* Limites + Opportunités */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr" }}>
        {/* Limites */}
        <div style={{
          padding: "10px 12px",
          borderRight: `0.5px solid ${C.bdr}`,
          background: "#e41a1c08",
        }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: "#e41a1c", marginBottom: 6, display: "flex", alignItems: "center", gap: 5 }}>
            <IcAlert size={12}/> Limites
          </div>
          <ul style={{ margin: 0, paddingLeft: 14, display: "flex", flexDirection: "column", gap: 4 }}>
            {kb.limits.map((l, i) => (
              <li key={i} style={{ color: C.dim, lineHeight: 1.4 }}>{l}</li>
            ))}
          </ul>
        </div>

        {/* Opportunités */}
        <div style={{
          padding: "10px 12px",
          background: "#4daf4a08",
        }}>
          <div style={{ fontSize: 10, fontWeight: 600, color: "#4daf4a", marginBottom: 6, display: "flex", alignItems: "center", gap: 5 }}>
            <IcCheck size={12}/> Atouts
          </div>
          <ul style={{ margin: 0, paddingLeft: 14, display: "flex", flexDirection: "column", gap: 4 }}>
            {kb.opportunities.map((o, i) => (
              <li key={i} style={{ color: C.dim, lineHeight: 1.4 }}>{o}</li>
            ))}
          </ul>
        </div>
      </div>

      {/* Conseil */}
      {kb.tip && (
        <div style={{
          padding: "8px 14px",
          background: C.hover,
          borderTop: `0.5px solid ${C.bdr}`,
          color: C.dim, fontStyle: "italic",
          borderRadius: "0 0 10px 10px",
          display: "flex", alignItems: "flex-start", gap: 6,
        }}>
          <IcSettings size={12} style={{ flexShrink: 0, marginTop: 2 }}/> <span>{kb.tip}</span>
        </div>
      )}
      {kb.note && (
        <div style={{
          padding: "8px 14px",
          background: C.hover,
          borderTop: `0.5px solid ${C.bdr}`,
          color: C.dim,
          borderRadius: "0 0 10px 10px",
        }}>
          {kb.note}
        </div>
      )}
    </div>
  );
}

// ── Note info-box ──────────────────────────────────────────────────────────────
function InfoBox({ icon: Icon, title, body, C }) {
  return (
    <div style={{
      padding:"12px 14px", borderRadius:8,
      background: C.hover, border:`0.5px solid ${C.bdr}`,
      display:"flex", gap:10,
    }}>
      <span style={{ flexShrink:0, color:C.acc, display:"flex", marginTop:1 }}>{Icon && <Icon size={17}/>}</span>
      <div>
        <div style={{ fontSize:11, fontWeight:600, color:C.txt, marginBottom:3 }}>{title}</div>
        <div style={{ fontSize:10, color:C.dim, lineHeight:1.5 }}>{body}</div>
      </div>
    </div>
  );
}

// ── Shell du modal ─────────────────────────────────────────────────────────────
function ModalShell({ title, subtitle, onClose, tabs, activeTab, setTab, children, C }) {
  const TAB = (id) => ({
    padding:"7px 14px", fontSize:11, border:"none", cursor:"pointer",
    fontFamily:F, borderRadius:"6px 6px 0 0",
    background: activeTab===id ? C.card : "transparent",
    color:      activeTab===id ? C.acc  : C.dim,
    fontWeight: activeTab===id ? 600    : 400,
  });
  return (
    <div style={{
      position:"fixed", inset:0, zIndex:9999,
      background:"rgba(0,0,0,.55)", display:"flex",
      alignItems:"center", justifyContent:"center",
    }} onClick={e=>{ if(e.target===e.currentTarget) onClose(); }}>
      <div style={{
        background:C.bg, borderRadius:14, padding:0, overflow:"hidden",
        width:"min(860px,95vw)", maxHeight:"88vh", display:"flex",
        flexDirection:"column", boxShadow:"0 20px 60px rgba(0,0,0,.4)",
        border:`1px solid ${C.bdr}`,
      }}>
        {/* Header */}
        <div style={{ padding:"16px 20px 0", display:"flex",
                      alignItems:"center", justifyContent:"space-between" }}>
          <div>
            <h3 style={{ margin:0, fontSize:14, fontFamily:F, color:C.txt }}>{title}</h3>
            {subtitle && <span style={{ fontSize:10, color:C.dim }}>{subtitle}</span>}
          </div>
          <button onClick={onClose} style={{
            background:"none", border:"none", cursor:"pointer",
            color:C.dim, padding:4, display:"flex",
          }}><IcX size={18}/></button>
        </div>

        {/* Tabs (optionnels) */}
        {tabs?.length > 0 && (
          <div style={{ display:"flex", padding:"12px 20px 0", gap:2,
                        borderBottom:`1px solid ${C.bdr}` }}>
            {tabs.map(([id,Icon,lbl]) => (
              <button key={id} style={{ ...TAB(id), display:"inline-flex", alignItems:"center", gap:5 }} onClick={()=>setTab(id)}><Icon size={12}/> {lbl}</button>
            ))}
          </div>
        )}

        {/* Body */}
        <div style={{ flex:1, overflowY:"auto", padding:"20px" }}>
          {children}
        </div>

        {/* Footer */}
        <div style={{ padding:"12px 20px", borderTop:`1px solid ${C.bdr}`,
                      display:"flex", justifyContent:"flex-end" }}>
          <button onClick={onClose} style={{
            fontFamily:F, padding:"7px 20px", borderRadius:8, border:"none",
            background:C.acc, color:"#fff", cursor:"pointer", fontSize:11,
          }}>Fermer</button>
        </div>
      </div>
    </div>
  );
}

// ── Graphe évolution multi-dates (barres empilées 100%) ───────────────────────
function EvolutionChart({ periodResults, C }) {
  if (!periodResults?.length) return null;

  // Calcul des pourcentages par période
  const data = periodResults.map(pr => {
    const total = (pr.legend || []).reduce((s, e) => s + (e.area_ha || 0), 0);
    return {
      label:   pr.label,
      total,
      entries: (pr.legend || []).map(e => ({
        ...e,
        pct: total > 0 ? (e.area_ha || 0) / total : 0,
      })),
    };
  });

  const BAR_W = 52, BAR_H = 130, GAP = 14;
  const SVG_W = periodResults.length * (BAR_W + GAP) + GAP;
  const SVG_H = BAR_H + 50;

  return (
    <div style={{ overflowX:"auto" }}>
      <svg width={SVG_W} height={SVG_H} style={{ fontFamily:F }}>
        {data.map((d, pi) => {
          const x = GAP + pi * (BAR_W + GAP);
          let yOff = 0;
          return (
            <g key={pi}>
              {d.entries.map((e, ei) => {
                if (e.pct <= 0) return null;
                const h  = e.pct * BAR_H;
                const y  = yOff;
                yOff    += h;
                const showLabel = h > 14;
                return (
                  <g key={ei}>
                    <rect x={x} y={y} width={BAR_W} height={h}
                          fill={e.color} stroke="#fff" strokeWidth={0.5}/>
                    {showLabel && (
                      <text x={x + BAR_W/2} y={y + h/2 + 4}
                            textAnchor="middle" fontSize={8} fill="#fff" fontWeight={600}>
                        {Math.round(e.pct * 100)}%
                      </text>
                    )}
                  </g>
                );
              })}
              {/* Label période */}
              <text x={x + BAR_W/2} y={BAR_H + 14} textAnchor="middle"
                    fontSize={11} fontWeight={600} fill={C.txt}>
                {d.label}
              </text>
              {d.total > 0 && (
                <text x={x + BAR_W/2} y={BAR_H + 28} textAnchor="middle"
                      fontSize={9} fill={C.dim}>
                  {(d.total / 100).toFixed(1)} km²
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

// ── Tableau évolution par classe ───────────────────────────────────────────────
function EvolutionTable({ periodResults, C }) {
  if (!periodResults?.length) return null;
  const classes = periodResults[0]?.legend || [];
  if (!classes.length) return null;

  // Variation entre première et dernière période
  const first = periodResults[0]?.legend || [];
  const last  = periodResults[periodResults.length - 1]?.legend || [];
  const totalFirst = first.reduce((s,e) => s + (e.area_ha||0), 0) || 1;
  const totalLast  = last.reduce((s,e)  => s + (e.area_ha||0), 0) || 1;

  return (
    <div style={{ overflowX:"auto" }}>
      <table style={{ width:"100%", borderCollapse:"collapse", fontSize:10, fontFamily:F }}>
        <thead>
          <tr style={{ borderBottom:`1px solid ${C.bdr}` }}>
            <th style={{ padding:"5px 8px", textAlign:"left", color:C.dim, fontWeight:500 }}>Classe</th>
            {periodResults.map(pr => (
              <th key={pr.label} style={{ padding:"5px 8px", textAlign:"right", color:C.dim, fontWeight:500 }}>
                {pr.label}
              </th>
            ))}
            <th style={{ padding:"5px 8px", textAlign:"right", color:C.dim, fontWeight:500 }}>Δ</th>
          </tr>
        </thead>
        <tbody>
          {classes.map((cls, ci) => {
            const pctFirst = first[ci]  ? (first[ci].area_ha  || 0) / totalFirst * 100 : 0;
            const pctLast  = last[ci]   ? (last[ci].area_ha   || 0) / totalLast  * 100 : 0;
            const delta    = pctLast - pctFirst;
            const dColor   = Math.abs(delta) < 1 ? C.dim : delta > 0 ? "#4daf4a" : "#e41a1c";
            return (
              <tr key={ci} style={{ borderBottom:`0.5px solid ${C.bdr}22` }}>
                <td style={{ padding:"6px 8px", display:"flex", alignItems:"center", gap:5 }}>
                  <div style={{ width:8, height:8, borderRadius:1, background:cls.color, flexShrink:0 }}/>
                  <span style={{ color:C.txt }}>{cls.label}</span>
                </td>
                {periodResults.map(pr => {
                  const e = pr.legend?.[ci];
                  const tot = (pr.legend || []).reduce((s,x)=>s+(x.area_ha||0),0)||1;
                  const pct = e ? Math.round((e.area_ha||0)/tot*100) : 0;
                  return (
                    <td key={pr.label} style={{ padding:"6px 8px", textAlign:"right", color:C.txt }}>
                      {pct}%
                      {e?.area_ha > 0 && (
                        <div style={{ fontSize:8, color:C.dim }}>
                          {fmtArea(e.area_ha)}
                        </div>
                      )}
                    </td>
                  );
                })}
                <td style={{ padding:"6px 8px", textAlign:"right", color:dColor, fontWeight:600 }}>
                  {delta > 0 ? "+" : ""}{delta.toFixed(1)}pp
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── Modal principal ────────────────────────────────────────────────────────────
export default function ClassifMetricsModal({ result, onClose }) {
  const C = useThemeContext();
  const [tab, setTab] = useState("summary");
  if (!result) return null;

  const { metrics, confusion_matrix, class_labels, feature_importance,
          legend, backend, bands_used, classifier_label,
          method, n_clusters,
          cloud_cover_pct, image_count } = result;

  // ════════════ MODE MULTI-DATES ════════════════════════════════════════════════
  if (backend === "gee_multidate") {
    const pr = result.period_results || [];
    const TABS_MD = [
      ["evolution", IcTrendingUp, "Évolution"],
      ["detail",    IcClipboard,  "Détail"],
    ];
    return (
      <ModalShell
        title="Classification multi-dates"
        subtitle={`${pr.length} période(s) · entraînement sur ${pr[0]?.label || "P1"}`}
        onClose={onClose} C={C} tabs={TABS_MD} activeTab={tab} setTab={setTab}>
        <div style={{ display:"flex", flexDirection:"column", gap:16 }}>

          {tab === "evolution" && (
            <>
              {/* Barres empilées 100% */}
              <div>
                <div style={{ fontSize:10, fontWeight:500, color:C.dim,
                              textTransform:"uppercase", letterSpacing:".05em", marginBottom:8 }}>
                  Répartition par période (% surfaces)
                </div>
                <EvolutionChart periodResults={pr} C={C} />
              </div>

              {/* Légende des classes */}
              <div style={{ display:"flex", flexWrap:"wrap", gap:6 }}>
                {(pr[0]?.legend || []).map(e => (
                  <div key={e.class_id} style={{ display:"flex", alignItems:"center", gap:5 }}>
                    <div style={{ width:10, height:10, borderRadius:2, background:e.color }}/>
                    <span style={{ fontSize:10, color:C.txt }}>{e.label}</span>
                  </div>
                ))}
              </div>

              {/* Tableau d'évolution */}
              <div>
                <div style={{ fontSize:10, fontWeight:500, color:C.dim,
                              textTransform:"uppercase", letterSpacing:".05em", marginBottom:8 }}>
                  Variation par classe (Δ = dernière − première période)
                </div>
                <EvolutionTable periodResults={pr} C={C} />
              </div>

              <InfoBox icon={IcInfo} title="Interprétation" C={C}
                body="Le classifieur est entraîné sur la 1ère période et appliqué aux suivantes.
Δ (points de pourcentage) mesure la variation de la part de chaque classe dans l'AOI entre la première et la dernière date." />
            </>
          )}

          {tab === "detail" && (
            <>
              {pr.map((period, pi) => (
                <div key={pi} style={{
                  padding:"12px", borderRadius:8,
                  background:C.hover, border:`0.5px solid ${C.bdr}`,
                }}>
                  <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginBottom:8 }}>
                    <div style={{ fontSize:12, fontWeight:700, color:C.txt, display:"flex", alignItems:"center", gap:5 }}>
                      <IcCalendar size={13}/> {period.label}
                    </div>
                    <div style={{ fontSize:9, color:C.dim }}>
                      {period.start} → {period.end}
                    </div>
                  </div>
                  {period.cloud_cover_pct != null && (
                    <div style={{ fontSize:9, color:C.dim, marginBottom:8, display:"flex", alignItems:"center", gap:5 }}>
                      <IcCloud size={11}/> {period.cloud_cover_pct}% couverture nuageuse · {period.image_count} image(s)
                    </div>
                  )}
                  <PieChart legend={period.legend} C={C} />
                </div>
              ))}
            </>
          )}
        </div>
      </ModalShell>
    );
  }

  // ════════════ MODE AUTO GEE (données pré-classifiées) ════════════════════════
  if (backend === "gee_auto") {
    const productKB = getProductKB(classifier_label);
    return (
      <ModalShell
        title="Données pré-classifiées GEE"
        subtitle={classifier_label ? `Classifieur : ${classifier_label}` : "Classification automatique"}
        onClose={onClose} C={C} tabs={[]} activeTab="" setTab={()=>{}}>
        <div style={{ display:"flex", flexDirection:"column", gap:16 }}>

          {/* Répartition surfacique — donut + % + km² */}
          <PieChart legend={legend} C={C} />

          {/* Fiche produit : description + limites + opportunités */}
          {productKB
            ? <KnowledgeCard kb={productKB} C={C} />
            : (
              <InfoBox icon={IcSatellite} title="Données pré-classifiées" C={C}
                body="Ces données sont produites par un modèle pré-entraîné sur Google Earth Engine.
Aucun entraînement n'est réalisé côté client — il n'y a donc pas de métriques de validation.
La qualité dépend du produit source (Dynamic World, WorldCover, MODIS…)." />
            )
          }

          {bands_used?.length > 0 && (
            <div style={{ fontSize:10, color:C.dim }}>
              Bande source : {bands_used.join(", ")}
            </div>
          )}

          <InfoBox icon={IcBulb} title="Pour obtenir des métriques" C={C}
            body="Utilisez l'onglet « Supervisée » avec des ROIs terrain
pour entraîner un classifieur personnalisé et obtenir accuracy, kappa et F1 par classe." />
        </div>
      </ModalShell>
    );
  }

  // ════════════ MODE CLUSTERING (non supervisé) ═════════════════════════════════
  if (backend === "gee_cluster") {
    const methodLabel = method === "xmeans"
      ? `X-Means (max ${n_clusters} clusters, nombre déterminé automatiquement)`
      : `K-Means (${n_clusters} clusters fixes)`;
    return (
      <ModalShell
        title="Classification non supervisée"
        subtitle={`GEE natif · ${methodLabel}`}
        onClose={onClose} C={C} tabs={[]} activeTab="" setTab={()=>{}}>
        <div style={{ display:"flex", flexDirection:"column", gap:16 }}>
          <InfoBox icon={IcCircleDot} title="Clustering non supervisé" C={C}
            body="Le clustering regroupe les pixels selon leur similarité spectrale, sans étiquettes terrain.
Il n'existe pas de métriques de validation classiques (accuracy, kappa) car il n'y a pas de vérité terrain.
La qualité s'évalue visuellement sur la carte et par interprétation thématique." />

          <CloudBadge cloudPct={cloud_cover_pct} imageCount={image_count} C={C} />

          {/* Méthode et paramètres */}
          <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:8 }}>
            <div style={{ padding:"10px 14px", borderRadius:8,
                          background:"#4A90D911", border:"1px solid #4A90D944" }}>
              <div style={{ fontSize:10, color:C.dim, marginBottom:4 }}>Méthode</div>
              <div style={{ fontSize:13, fontWeight:700, color:"#4A90D9" }}>
                {method === "xmeans" ? "X-Means" : "K-Means"}
              </div>
            </div>
            <div style={{ padding:"10px 14px", borderRadius:8,
                          background:"#4daf4a11", border:"1px solid #4daf4a44" }}>
              <div style={{ fontSize:10, color:C.dim, marginBottom:4 }}>Clusters trouvés</div>
              <div style={{ fontSize:13, fontWeight:700, color:"#4daf4a" }}>
                {n_clusters}
              </div>
            </div>
          </div>

          {bands_used?.length > 0 && (
            <div style={{ fontSize:10, color:C.dim }}>
              Bandes utilisées : {bands_used.join(", ")}
            </div>
          )}

          {/* Répartition surfacique — donut + % + km² */}
          <PieChart legend={legend} C={C} />
          <div style={{ fontSize:9, color:C.dim, display:"flex", alignItems:"center", gap:5 }}>
            <IcBulb size={11}/> Couleurs éditables dans le Gestionnaire de couches
          </div>

          {/* Fiche clustering : limites + opportunités */}
          <KnowledgeCard C={C} kb={{
            icon: method === "xmeans" ? IcCircleDot : IcGrid3,
            name: method === "xmeans" ? "X-Means (nombre de clusters automatique)" : `K-Means (k = ${n_clusters} fixe)`,
            description: method === "xmeans"
              ? "X-Means explore automatiquement le meilleur nombre de clusters en utilisant le critère BIC. " +
                "Il divise chaque cluster tant que la qualité s'améliore."
              : "K-Means regroupe les pixels en k clusters en minimisant l'inertie intra-classe. " +
                "Algorithme classique de clustering spectral, simple et rapide.",
            limits: method === "xmeans"
              ? [
                  "Peut sur-segmenter si la limite max est trop élevée",
                  "Résultat variable selon l'initialisation aléatoire",
                  "Plus lent que K-Means classique",
                ]
              : [
                  "Nombre de clusters k à choisir a priori (peut être sous- ou sur-estimé)",
                  "Sensible à l'initialisation (minimum local possible)",
                  "Suppose des clusters sphériques de taille similaire",
                ],
            opportunities: [
              "Aucune donnée terrain requise — exploration libre",
              "Regroupe les pixels selon leur similarité spectrale réelle",
              "Résultat rapide sur toute la zone AOI via GEE",
              "Utile pour découvrir des sous-unités spectrales inattendues",
            ],
            tip: "Croisez visuellement les clusters avec la carte de base pour les identifier " +
              "(végétation dense, eau, bâti…), puis renommez-les dans le Gestionnaire de couches.",
          }} />
        </div>
      </ModalShell>
    );
  }

  // ════════════ MODE SUPERVISÉ (GEE natif ou sklearn) ══════════════════════════
  const { overall_accuracy, kappa, per_class } = metrics || {};

  // Identifier le modèle à partir de result.classifier ou backend
  const modelId = result.classifier_id || result.model || result.classifier_label || "";
  const modelKB  = MODEL_KB[modelId] || null;

  const TABS = [
    ["summary",    IcBarChart,   "Résumé"],
    ["matrix",     IcGrid3,      "Matrice"],
    ["importance", IcTrendingUp, "Importance"],
    ["model",      IcInfo,       "Modèle"],
  ];

  return (
    <ModalShell
      title="Résultats de classification supervisée"
      subtitle={`Backend : ${backend === "gee" ? "GEE natif" : "sklearn local"}${bands_used?.length ? ` · ${bands_used.length} bande(s)` : ""}`}
      onClose={onClose} C={C} tabs={TABS} activeTab={tab} setTab={setTab}>

      {tab === "summary" && (
        <div style={{ display:"flex", flexDirection:"column", gap:16 }}>

          {/* Qualité image (cloud coverage) */}
          <CloudBadge cloudPct={cloud_cover_pct} imageCount={image_count} C={C} />

          {/* Badges accuracy / kappa */}
          <div style={{ display:"flex", gap:12, justifyContent:"center" }}>
            <AccBadge value={overall_accuracy} label="Accuracy" />
            <AccBadge value={kappa>=0?(kappa+1)/2:0} label={`Kappa ${kappa?.toFixed(3)||"—"}`} />
          </div>

          {/* Note : resubstitution */}
          {backend === "gee" && (
            <div style={{
              fontSize:9, color:C.dim, textAlign:"center",
              padding:"4px 10px", borderRadius:4,
              background:C.hover, border:`0.5px solid ${C.bdr}`,
            }}>
              Accuracy calculée sur l'ensemble des échantillons d'entraînement (resubstitution)
              — légèrement optimiste. Pour une validation rigoureuse, augmentez la taille des ROIs.
            </div>
          )}

          {/* Répartition surfacique (donut) */}
          <PieChart legend={legend} C={C} />

          {/* Tableau per-class */}
          <div style={{ overflowX:"auto" }}>
            <table style={{ width:"100%", borderCollapse:"collapse", fontFamily:F, fontSize:11 }}>
              <thead>
                <tr style={{ borderBottom:`1px solid ${C.bdr}` }}>
                  {["Classe","Précision","Rappel","F1","Support"].map(h=>(
                    <th key={h} style={{ padding:"6px 10px", textAlign:"left",
                                        color:C.dim, fontWeight:500 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(per_class||[]).map((row,i)=>{
                  const lc=legend?.[i]?.color||"#888";
                  return (
                    <tr key={row.class_id} style={{ borderBottom:`1px solid ${C.bdr}22` }}>
                      <td style={{ padding:"8px 10px", display:"flex",
                                   alignItems:"center", gap:6 }}>
                        <span style={{ width:8, height:8, borderRadius:"50%",
                                       background:lc, flexShrink:0 }}/>
                        <strong style={{ color:C.txt }}>{row.label}</strong>
                      </td>
                      {["precision","recall","f1"].map(k=>(
                        <td key={k} style={{ padding:"8px 10px",
                          color:row[k]>=0.8?"#4daf4a":row[k]>=0.6?"#e07b00":"#e41a1c" }}>
                          {Math.round((row[k]||0)*100)}%
                        </td>
                      ))}
                      <td style={{ padding:"8px 10px", color:C.dim }}>
                        {row.support ?? "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tab === "matrix" && (
        <div style={{ display:"flex", flexDirection:"column", gap:12 }}>
          <p style={{ fontSize:11, color:C.dim, margin:0 }}>
            Lignes = classe réelle · Colonnes = classe prédite ·{" "}
            <strong style={{ color:C.txt }}>Diagonale = bonnes classifications</strong>
          </p>
          <ConfusionMatrix matrix={confusion_matrix} labels={class_labels||[]} C={C} />
        </div>
      )}

      {tab === "importance" && (
        <div style={{ display:"flex", flexDirection:"column", gap:12 }}>
          <p style={{ fontSize:11, color:C.dim, margin:0 }}>
            Contribution de chaque bande/indice à la classification
            (Random Forest · CART · Gradient Boosting uniquement)
          </p>
          <FeatureImportance data={feature_importance} C={C} />
        </div>
      )}

      {tab === "model" && (
        <div style={{ display:"flex", flexDirection:"column", gap:16 }}>
          {modelKB
            ? <KnowledgeCard kb={modelKB} C={C} />
            : (
              <InfoBox icon={IcInfo} title="Informations modèle" C={C}
                body={`Modèle : ${modelId || "inconnu"}.
Les métriques d'accuracy, kappa et F1 sont calculées en resubstitution sur les échantillons d'entraînement.
Pour une validation indépendante, réservez 20–30 % des ROIs comme données de test.`} />
            )
          }
          <InfoBox icon={IcMicroscope} title="Validation et bonnes pratiques" C={C}
            body={`• Accuracy = (VP + VN) / Total — sensible au déséquilibre entre classes.
• Kappa de Cohen (0–1) : corrige le hasard — visez > 0.7 pour une bonne classification.
• F1 par classe : équilibre précision et rappel — surveiller les classes rares (F1 < 0.5).
• Pour les zones GEE, augmentez la taille des ROIs (> 100 pixels/classe) pour réduire le biais de resubstitution.`} />
        </div>
      )}
    </ModalShell>
  );
}
