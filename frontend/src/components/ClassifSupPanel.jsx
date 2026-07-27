/**
 * ClassifSupPanel.jsx — Classification supervisée + Auto GEE
 *
 *  • Polygone visible dès le 1er point (pts / edges / close / fill)
 *  • Color picker par classe avant ET après classification
 *  • Restyle sans ré-entraînement via /api/gee/classify/restyle
 *  • Mode "Auto GEE" : classifieurs pré-entraînés (DynamicWorld, WorldCover, MODIS, Copernicus)
 *  • Import fichier GeoJSON comme AOI
 *
 * Props : mapRef, layers, addRasterLayer, classifClickRef
 */
import { useState, useEffect, useRef, useCallback } from "react";
import * as turf from "@turf/turf";
import { useThemeContext } from "../theme";
import { F } from "../config";
import ClassifMetricsModal from "./ClassifMetricsModal";
import { IcTreePine, IcShuffle, IcRocket, IcScissors, IcMapPin, IcBarChart,
  IcCheck, IcX, IcEdit, IcFolder, IcMouse, IcTrash, IcSatellite, IcCalendar,
  IcCloud, IcBulb, IcInfo, IcRefreshCw, IcClassif, IcZap, IcCircleDot, IcMap,
  IcLoader, IcAlert } from "../icons";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ── Palette ───────────────────────────────────────────────────────────────────
const CLASS_COLORS = [
  "#e41a1c","#377eb8","#4daf4a","#984ea3",
  "#ff7f00","#a65628","#f781bf","#009999",
  "#ffff33","#66cc00",
];

// ── Modèles supervisés ────────────────────────────────────────────────────────
const MODELS = [
  { id:"smileRandomForest", label:"Random Forest", icon:IcTreePine, desc:"Robuste, standard", importance:true,
    params:[
      {key:"numberOfTrees",    label:"Nb arbres",    type:"number", default:100, min:10,  max:500},
      {key:"minLeafPopulation",label:"Min feuille",  type:"number", default:1,   min:1,   max:50},
      {key:"bagFraction",      label:"Bag fraction", type:"number", default:0.5, min:0.1, max:1, step:0.1},
    ]},
  { id:"smileCart", label:"Arbre décision", icon:IcShuffle, desc:"Interprétable, rapide", importance:true,
    params:[
      {key:"minLeafPopulation",label:"Min feuille",type:"number",default:1,min:1, max:50},
      {key:"maxNodes",         label:"Max nœuds", type:"number",default:0,min:0, max:1000, hint:"0=illimité"},
    ]},
  { id:"smileGradientTreeBoost", label:"Gradient Boost", icon:IcRocket, desc:"Haute précision", importance:true,
    params:[
      {key:"numberOfTrees",label:"Estimateurs",  type:"number",default:100, min:10,   max:300},
      {key:"shrinkage",    label:"Learning rate",type:"number",default:0.05,min:0.001,max:0.5, step:0.01},
      {key:"samplingRate", label:"Subsampling",  type:"number",default:0.7, min:0.3,  max:1,   step:0.1},
    ]},
  { id:"libsvm", label:"SVM", icon:IcScissors, desc:"Petits datasets", importance:false,
    params:[
      {key:"kernelType",label:"Kernel",   type:"select",options:["RBF","LINEAR","POLY","SIGMOID"],default:"RBF"},
      {key:"cost",      label:"C (régul.)",type:"number",default:1.0,min:0.01,max:100,step:0.1},
    ]},
  { id:"minimumDistance", label:"Min. Distance", icon:IcMapPin, desc:"KNN GEE natif", importance:false,
    params:[
      {key:"metric",label:"Métrique",type:"select",options:["euclidean","cosine","manhattan"],default:"euclidean"},
      {key:"k",     label:"Voisins k",type:"number",default:1,min:1,max:10},
    ]},
  { id:"smileNaiveBayes", label:"Naïve Bayes", icon:IcBarChart, desc:"Rapide, probabiliste", importance:false,
    params:[{key:"lambda_",label:"Lissage λ",type:"number",default:1.0,min:0,max:10,step:0.1}]},
];

const STEPS = [{id:1,label:"Zone"},{id:2,label:"ROIs"},{id:3,label:"Modèle"},{id:4,label:"Résultat"}];

// ── Helpers ───────────────────────────────────────────────────────────────────
function computeArea(pts) {
  if (pts.length < 3) return 0;
  try { return turf.area(turf.polygon([[...pts, pts[0]]])) / 1e6; } catch { return 0; }
}
function uniqueId(p="id") { return `${p}_${Date.now()}_${Math.random().toString(36).slice(2,6)}`; }

// ── GeoJSON pour le dessin interactif (AOI) ───────────────────────────────────
// Produit Points + LineString(edges) + LineString(close) + Polygon dès le 1er clic
function buildDrawFC(pts) {
  const f = [];
  if (!pts.length) return { type:"FeatureCollection", features:f };
  // Point markers (dès le 1er clic)
  pts.forEach(p => f.push({
    type:"Feature", geometry:{type:"Point",coordinates:p}, properties:{k:"pt"},
  }));
  if (pts.length >= 2) {
    // Arêtes entre les points consécutifs
    f.push({ type:"Feature", geometry:{type:"LineString",coordinates:pts}, properties:{k:"edges"} });
    // Arête de fermeture (dernier → premier) en pointillés
    f.push({ type:"Feature", geometry:{type:"LineString",coordinates:[pts[pts.length-1],pts[0]]}, properties:{k:"close"} });
  }
  if (pts.length >= 3) {
    // Remplissage polygone
    f.push({ type:"Feature", geometry:{type:"Polygon",coordinates:[[...pts,pts[0]]]}, properties:{k:"poly"} });
  }
  return { type:"FeatureCollection", features:f };
}

// ── GeoJSON pour les ROIs + polygone en cours ─────────────────────────────────
function buildRoisFC(classes, curPts, drawingFor) {
  const f = [];
  // ROIs finalisées
  classes.forEach(cls => {
    cls.rois.forEach(roi => {
      f.push({ type:"Feature", geometry:roi, properties:{k:"roi",color:cls.color} });
    });
  });
  // Polygone en cours de dessin — points + lignes UNIQUEMENT (pas de fill pendant le dessin)
  if (drawingFor !== null && curPts.length > 0) {
    const color = classes.find(c=>c.id===drawingFor)?.color || "#888";
    // Points visibles dès le 1er clic
    curPts.forEach(p => f.push({ type:"Feature", geometry:{type:"Point",coordinates:p}, properties:{k:"cp",color} }));
    if (curPts.length >= 2) {
      f.push({ type:"Feature", geometry:{type:"LineString",coordinates:curPts}, properties:{k:"ce",color} });
      // Ligne de fermeture pointillée (dernier → premier)
      f.push({ type:"Feature", geometry:{type:"LineString",coordinates:[curPts[curPts.length-1],curPts[0]]}, properties:{k:"cc",color} });
    }
    // Pas de fill (k:"cp2") pendant le dessin — seulement après validation du ROI
  }
  return { type:"FeatureCollection", features:f };
}

// ── Définitions des couches MapLibre ─────────────────────────────────────────
const AOI_COLOR = "#4A90D9";

const AOI_LAYERS = [
  { id:"classif-aoi-fill",  type:"fill",   filter:["==","$type","Polygon"],
    paint:{"fill-color":AOI_COLOR,"fill-opacity":0.12} },
  { id:"classif-aoi-ol",    type:"line",   filter:["==","$type","Polygon"],
    paint:{"line-color":AOI_COLOR,"line-width":2} },
  { id:"classif-aoi-edges", type:"line",
    filter:["all",["==","$type","LineString"],["==",["get","k"],"edges"]],
    paint:{"line-color":AOI_COLOR,"line-width":2} },
  { id:"classif-aoi-close", type:"line",
    filter:["all",["==","$type","LineString"],["==",["get","k"],"close"]],
    paint:{"line-color":AOI_COLOR,"line-width":1.5,"line-dasharray":[4,3]} },
  { id:"classif-aoi-pts",   type:"circle", filter:["==","$type","Point"],
    paint:{"circle-radius":4,"circle-color":AOI_COLOR,"circle-stroke-width":1.5,"circle-stroke-color":"#fff"} },
];

const ROI_LAYERS = [
  { id:"classif-roi-fill",   type:"fill",
    filter:["==",["get","k"],"roi"],
    paint:{"fill-color":["get","color"],"fill-opacity":0.3} },
  { id:"classif-roi-ol",     type:"line",
    filter:["==",["get","k"],"roi"],
    paint:{"line-color":["get","color"],"line-width":1.5} },
  { id:"classif-roi-cfill",  type:"fill",
    filter:["==",["get","k"],"cp2"],
    paint:{"fill-color":["get","color"],"fill-opacity":0.2} },
  { id:"classif-roi-cedges", type:"line",
    filter:["all",["==","$type","LineString"],["==",["get","k"],"ce"]],
    paint:{"line-color":["get","color"],"line-width":2} },
  { id:"classif-roi-cclose", type:"line",
    filter:["all",["==","$type","LineString"],["==",["get","k"],"cc"]],
    paint:{"line-color":["get","color"],"line-width":1.5,"line-dasharray":[3,2]} },
  { id:"classif-roi-cpts",   type:"circle",
    filter:["all",["==","$type","Point"],["==",["get","k"],"cp"]],
    paint:{"circle-radius":4,"circle-color":["get","color"],"circle-stroke-width":1.5,"circle-stroke-color":"#fff"} },
];

// ── Sync / remove MapLibre sources ────────────────────────────────────────────
function syncSrc(map, srcId, layerDefs, data) {
  if (!map?.isStyleLoaded()) return;
  if (map.getSource(srcId)) {
    map.getSource(srcId).setData(data);
  } else {
    map.addSource(srcId, { type:"geojson", data });
    layerDefs.forEach(({ id, type, filter, paint }) => {
      if (!map.getLayer(id)) map.addLayer({ id, type, source:srcId, filter, paint });
    });
  }
}
function removeSrc(map, srcId, layerIds) {
  if (!map) return;
  try {
    layerIds.forEach(id => { if (map.getLayer(id)) map.removeLayer(id); });
    if (map.getSource(srcId)) map.removeSource(srcId);
  } catch(_) {}
}

// ── Stepper ───────────────────────────────────────────────────────────────────
function Stepper({ step, C }) {
  return (
    <div style={{ display:"flex", alignItems:"center", marginBottom:10 }}>
      {STEPS.map((s, i) => (
        <div key={s.id} style={{ display:"flex", alignItems:"center", flex:1 }}>
          <div style={{
            width:20, height:20, borderRadius:"50%", flexShrink:0,
            display:"flex", alignItems:"center", justifyContent:"center",
            fontSize:10, fontWeight:600, fontFamily:F,
            background: step >= s.id ? "#22a558" : C.hover,
            color:      step >= s.id ? "#fff"    : C.dim,
          }}>{step > s.id ? <IcCheck size={12}/> : s.id}</div>
          <span style={{ fontSize:9, color:step >= s.id ? C.txt : C.dim, marginLeft:3, whiteSpace:"nowrap" }}>
            {s.label}
          </span>
          {i < STEPS.length - 1 && (
            <div style={{ flex:1, height:1, background:step > s.id ? "#4A90D9" : C.bdr, margin:"0 4px" }}/>
          )}
        </div>
      ))}
    </div>
  );
}

// ── Composant principal ───────────────────────────────────────────────────────
export default function ClassifSupPanel({ mapRef, layers, addRasterLayer, updateRasterLayer, classifClickRef }) {
  const C = useThemeContext();

  // ── Mode ────────────────────────────────────────────────────────────────
  const [mode, setMode] = useState("supervised"); // "supervised" | "auto"

  // ── État général ────────────────────────────────────────────────────────
  const [step,        setStep]        = useState(1);
  const [error,       setError]       = useState(null);
  const [loading,     setLoading]     = useState(false);
  const [result,      setResult]      = useState(null);
  const [showMetrics, setShowMetrics] = useState(false);

  // ── AOI ─────────────────────────────────────────────────────────────────
  const [selLayerId, setSelLayerId] = useState("");
  const [aoiPoints,  setAoiPoints]  = useState([]);
  const [aoiDrawing, setAoiDrawing] = useState(false);
  const [aoiPoly,    setAoiPoly]    = useState(null);
  const aoiFileRef = useRef(null);

  // ── ROIs ─────────────────────────────────────────────────────────────────
  const [roiMode,    setRoiMode]    = useState("manual");
  const [classes,    setClasses]    = useState([]);
  const [drawingFor, setDrawingFor] = useState(null);
  const [curPts,     setCurPts]     = useState([]);
  const [vecLayerId, setVecLayerId] = useState("");
  const [vecAttr,    setVecAttr]    = useState("");
  const lastVecKey = useRef("");

  // ── Modèle ──────────────────────────────────────────────────────────────
  const [modelId,     setModelId]     = useState("smileRandomForest");
  const [modelParams, setModelParams] = useState({});
  const [trainRatio,  setTrainRatio]  = useState(0.7);

  // ── Multi-dates ──────────────────────────────────────────────────────────
  const [multiDate,       setMultiDate]       = useState(false);
  const [periods,         setPeriods]         = useState([
    { start:"2022-01-01", end:"2022-12-31", label:"2022" },
    { start:"2023-01-01", end:"2023-12-31", label:"2023" },
  ]);
  const [activePeriodIdx, setActivePeriodIdx] = useState(0);

  // ── Auto GEE ────────────────────────────────────────────────────────────
  const [autoList,      setAutoList]      = useState([]);
  const [autoId,        setAutoId]        = useState("");
  const [autoDateStart, setAutoDateStart] = useState("2023-01-01");
  const [autoDateEnd,   setAutoDateEnd]   = useState("2023-12-31");
  const [autoLoading,   setAutoLoading]   = useState(false);
  const [autoError,     setAutoError]     = useState(null);
  const [autoStep,      setAutoStep]      = useState(1);

  // ── Non supervisé (clustering) ───────────────────────────────────────────
  const [clusterMethod,  setClusterMethod]  = useState("kmeans"); // "kmeans" | "xmeans"
  const [nClusters,      setNClusters]      = useState(5);
  const [maxClusters,    setMaxClusters]    = useState(10);
  const [clusterLoading, setClusterLoading] = useState(false);
  const [clusterError,   setClusterError]   = useState(null);
  const [clusterStep,    setClusterStep]    = useState(1);

  // ── Restyle ─────────────────────────────────────────────────────────────
  const [restyleColors,  setRestyleColors]  = useState([]);
  const [restyleLoading, setRestyleLoading] = useState(false);
  const currentLayerId     = useRef(null);
  const currentLayerBounds = useRef(null);
  const currentLayerIsGee  = useRef(false);

  // ── Couches disponibles ──────────────────────────────────────────────────
  const rasterLayers = layers.filter(l => l.isRaster && l.visible);
  const vectorLayers = layers.filter(l => !l.isRaster);
  const selLayer     = layers.find(l => l.id === selLayerId);
  // isGee : couche GEE native (a des _geeParams) OU mode Auto GEE (pas de couche source)
  const isGee        = mode === "auto" || !!(selLayer?._geeParams);
  const aoiArea      = aoiPoly
    ? computeArea(aoiPoly.coordinates[0].slice(0, -1))
    : computeArea(aoiPoints);

  const getMap = useCallback(() => mapRef.current?.getMap?.(), [mapRef]);

  // ── Fetch auto-classifiers ───────────────────────────────────────────────
  useEffect(() => {
    fetch(`${API}/api/gee/auto-classifiers`)
      .then(r => r.json())
      .then(d => { setAutoList(d); if (d.length) setAutoId(d[0].id); })
      .catch(() => {});
  }, []);

  // ── Init classes depuis couche vecteur ───────────────────────────────────
  useEffect(() => {
    if (roiMode !== "layer" || !vecLayerId || !vecAttr) return;
    const key = `${vecLayerId}:${vecAttr}`;
    if (lastVecKey.current === key) return;
    lastVecKey.current = key;
    const vl = layers.find(l => l.id === vecLayerId);
    const vals = [...new Set((vl?.geojson?.features || [])
      .map(f => String(f.properties?.[vecAttr] || "")))].slice(0, 20);
    if (!vals.length) return;
    setClasses(vals.map((v, i) => ({
      id: i, label: v, color: CLASS_COLORS[i % CLASS_COLORS.length], rois: [],
    })));
  }, [roiMode, vecLayerId, vecAttr, layers]);

  // ── Curseur carte ────────────────────────────────────────────────────────
  useEffect(() => {
    const map = getMap(); if (!map) return;
    map.getCanvas().style.cursor = (aoiDrawing || drawingFor !== null) ? "crosshair" : "";
    return () => { try { map.getCanvas().style.cursor = ""; } catch(_) {} };
  }, [aoiDrawing, drawingFor, getMap]);

  // ── Handler de clic ──────────────────────────────────────────────────────
  useEffect(() => {
    if (aoiDrawing) {
      classifClickRef.current = (lng, lat) => setAoiPoints(p => [...p, [lng, lat]]);
    } else if (drawingFor !== null) {
      classifClickRef.current = (lng, lat) => setCurPts(p => [...p, [lng, lat]]);
    } else {
      classifClickRef.current = null;
    }
    return () => { classifClickRef.current = null; };
  }, [aoiDrawing, drawingFor, classifClickRef]);

  // ── Sync source MapLibre AOI ─────────────────────────────────────────────
  useEffect(() => {
    const map = getMap(); if (!map) return;
    let fc;
    if (aoiDrawing) {
      fc = buildDrawFC(aoiPoints);
    } else if (aoiPoly) {
      // Polygone finalisé : seulement fill + outline (pas de points/dashes)
      fc = { type:"FeatureCollection", features:[
        { type:"Feature", geometry:aoiPoly, properties:{ k:"poly" } }
      ]};
    } else {
      fc = { type:"FeatureCollection", features:[] };
    }
    const doSync = () => syncSrc(map, "classif-aoi", AOI_LAYERS, fc);
    if (map.isStyleLoaded()) doSync(); else map.once("load", doSync);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [aoiDrawing, aoiPoints, aoiPoly]);

  // ── Sync source MapLibre ROI ─────────────────────────────────────────────
  useEffect(() => {
    const map = getMap(); if (!map) return;
    const fc = buildRoisFC(classes, curPts, drawingFor);
    const doSync = () => syncSrc(map, "classif-roi", ROI_LAYERS, fc);
    if (map.isStyleLoaded()) doSync(); else map.once("load", doSync);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [classes, curPts, drawingFor]);

  // ── Cleanup au démontage ─────────────────────────────────────────────────
  useEffect(() => {
    return () => {
      classifClickRef.current = null;
      const map = getMap(); if (!map) return;
      removeSrc(map, "classif-aoi", AOI_LAYERS.map(l => l.id));
      removeSrc(map, "classif-roi", ROI_LAYERS.map(l => l.id));
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Actions AOI ──────────────────────────────────────────────────────────
  const startAoi = () => { setAoiPoints([]); setAoiPoly(null); setAoiDrawing(true); setError(null); };
  const resetAoi = () => { setAoiPoints([]); setAoiPoly(null); setAoiDrawing(false); };
  const closeAoi = () => {
    if (aoiPoints.length < 3) { setError("Minimum 3 points"); return; }
    if (!isGee && aoiArea > 105) {
      setError(`Zone trop grande (${aoiArea.toFixed(2)} km²) — max 100 km² pour WMS/Tiles`); return;
    }
    setAoiPoly({ type:"Polygon", coordinates:[[...aoiPoints, aoiPoints[0]]] });
    setAoiDrawing(false); setError(null);
  };

  const handleAoiFile = e => {
    const file = e.target.files?.[0]; if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => {
      try {
        const gj = JSON.parse(ev.target.result);
        let poly = null;
        if (gj.type === "Polygon") poly = gj;
        else if (gj.type === "Feature" && gj.geometry?.type === "Polygon") poly = gj.geometry;
        else if (gj.type === "FeatureCollection") {
          const feat = gj.features?.find(f =>
            f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
          );
          if (feat?.geometry?.type === "MultiPolygon")
            poly = { type:"Polygon", coordinates:feat.geometry.coordinates[0] };
          else poly = feat?.geometry || null;
        }
        if (!poly) { setError("Aucun polygone trouvé dans le fichier"); return; }
        const ring = poly.coordinates[0];
        const pts  = ring.slice(0, -1);
        if (!isGee) {
          const a = computeArea(pts);
          if (a > 105) { setError(`Zone trop grande (${a.toFixed(2)} km²) — max 100 km²`); return; }
        }
        setAoiPoints(pts); setAoiPoly(poly); setAoiDrawing(false); setError(null);
      } catch { setError("Fichier GeoJSON invalide"); }
    };
    reader.readAsText(file);
    e.target.value = "";
  };

  // ── Actions Classes ──────────────────────────────────────────────────────
  const addClass      = () => {
    const id = Date.now();
    setClasses(p => [...p, { id, label:`Classe ${p.length+1}`, color:CLASS_COLORS[p.length % CLASS_COLORS.length], rois:[] }]);
  };
  const renameClass   = (id, label)  => setClasses(p => p.map(c => c.id===id ? {...c,label}   : c));
  const setClassColor = (id, color)  => setClasses(p => p.map(c => c.id===id ? {...c,color}   : c));
  const removeClass   = id           => setClasses(p => p.filter(c => c.id !== id));

  // ── Actions périodes multi-dates ────────────────────────────────────────
  const addPeriod    = () => {
    const yr = (new Date().getFullYear() - periods.length + 1).toString();
    setPeriods(p => [...p, { start:`${yr}-01-01`, end:`${yr}-12-31`, label:yr }]);
  };
  const removePeriod = i  => setPeriods(p => p.filter((_,j) => j !== i));
  const updatePeriod = (i, key, val) => setPeriods(p => p.map((x,j) => j===i ? {...x,[key]:val} : x));

  // ── Switcher période sur la carte ────────────────────────────────────────
  const switchPeriodOnMap = (idx) => {
    if (!result?.period_results) return;
    const pr  = result.period_results[idx];
    const src = currentLayerId.current;
    if (!src || !pr?.tile_url) return;

    const map = getMap();
    if (map) {
      try {
        const source = map.getSource(src);
        if (source?.setTiles) {
          // MapLibre GL ≥ 3 : swap tuiles sans supprimer la couche (pas de clignotement)
          source.setTiles([pr.tile_url]);
        } else {
          // Fallback : remove + re-add
          const lyrId = `${src}-lyr`;
          try { if (map.getLayer(lyrId)) map.removeLayer(lyrId); } catch(_) {}
          try { if (map.getSource(src))  map.removeSource(src);  } catch(_) {}
          map.addSource(src, { type:"raster", tiles:[pr.tile_url], tileSize:256 });
          map.addLayer({ id:lyrId, type:"raster", source:src, paint:{"raster-opacity":0.85} });
        }
      } catch(ex) { console.warn("switchPeriod:", ex); }
    }

    // Mettre à jour la légende dans le panneau gauche (pas de swap MapLibre ici)
    if (updateRasterLayer) {
      updateRasterLayer(src, { legend: pr.legend || [] });
    }

    setActivePeriodIdx(idx);
  };

  // ── Actions ROI ──────────────────────────────────────────────────────────
  const startRoi  = id  => { setDrawingFor(id); setCurPts([]); setError(null); };
  const cancelRoi = ()  => { setCurPts([]); setDrawingFor(null); };
  const closeRoi  = ()  => {
    if (curPts.length < 3) { setError("Minimum 3 points"); return; }
    const poly = { type:"Polygon", coordinates:[[...curPts, curPts[0]]] };
    setClasses(p => p.map(c => c.id === drawingFor ? {...c, rois:[...c.rois, poly]} : c));
    setCurPts([]); setDrawingFor(null); setError(null);
  };
  const removeRoi = (classId, idx) => setClasses(p =>
    p.map(c => c.id === classId ? {...c, rois:c.rois.filter((_,i) => i !== idx)} : c)
  );

  // ── Modèle helpers ───────────────────────────────────────────────────────
  const modelDef = MODELS.find(m => m.id === modelId) || MODELS[0];
  const getParam = k  => modelParams[k] ?? modelDef.params.find(p => p.key === k)?.default;
  const setParam = (k, v) => setModelParams(p => ({...p, [k]:v}));

  // ── Validation étapes ────────────────────────────────────────────────────
  const canGoStep2 = !!(selLayerId && aoiPoly);
  const canGoStep3 = roiMode === "layer"
    ? (vecLayerId && vecAttr && classes.length >= 2)
    : (classes.length >= 2 && classes.every(c => c.rois.length > 0));

  // ── Ajout couche résultat sur la carte ───────────────────────────────────
  const addResultLayer = useCallback((data, layerId, layerName) => {
    const map = getMap();
    currentLayerId.current = layerId;
    currentLayerBounds.current = data.image_bounds;
    currentLayerIsGee.current = !!data.tile_url;

    const layerBase = {
      id:layerId, name:layerName, type:"classif",
      bbox:data.image_bounds, opacity:0.85,
      legend:  data.legend   || null,   // stocké pour LayerPanel
      job_id:  data.job_id   || null,   // stocké pour restyle depuis LayerPanel
    };

    if (data.tile_url && map) {
      try {
        map.addSource(layerId, { type:"raster", tiles:[data.tile_url], tileSize:256 });
        map.addLayer({ id:`${layerId}-lyr`, type:"raster", source:layerId,
                       paint:{"raster-opacity":0.85} });
      } catch(ex) { console.warn("addSource classif GEE:", ex); }
      addRasterLayer({ ...layerBase, tileUrl:data.tile_url });
    } else if (data.image_url && data.image_bounds && map) {
      const [W, S, E, N] = data.image_bounds;
      try {
        map.addSource(layerId, { type:"image", url:data.image_url,
                                  coordinates:[[W,N],[E,N],[E,S],[W,S]] });
        map.addLayer({ id:`${layerId}-lyr`, type:"raster", source:layerId,
                       paint:{"raster-opacity":0.85} });
      } catch(ex) { console.warn("addSource classif WMS:", ex); }
      addRasterLayer({ ...layerBase, tileUrl:null });
    }
  }, [getMap, addRasterLayer]);

  // ── Lancer classification supervisée ─────────────────────────────────────
  const run = async () => {
    setLoading(true); setError(null); setResult(null);
    try {
      let rois = [];
      if (roiMode === "manual") {
        classes.forEach((cls, idx) => {
          cls.rois.forEach(roi => {
            // class_id = index numérique (0,1,2…) — compatible Pydantic int et sklearn
            rois.push({ geometry:roi, label:cls.label, class_id:idx, color:cls.color });
          });
        });
      } else {
        const vl = layers.find(l => l.id === vecLayerId);
        const feats = vl?.geojson?.features || [];
        const labels = [...new Set(feats.map(f => String(f.properties?.[vecAttr] || "")))];
        labels.forEach((lbl, idx) => {
          feats.filter(f => String(f.properties?.[vecAttr] || "") === lbl).forEach(f => {
            const geom = f.geometry;
            if (geom?.type === "Polygon" || geom?.type === "MultiPolygon") {
              const g = geom.type === "MultiPolygon"
                ? { type:"Polygon", coordinates:geom.coordinates[0] } : geom;
              const cls = classes.find(c => c.label === lbl);
              rois.push({ geometry:g, label:lbl, class_id:idx,
                          color: cls?.color || CLASS_COLORS[idx % CLASS_COLORS.length] });
            }
          });
        });
      }
      if (rois.length < 2) throw new Error("Minimum 2 ROIs dans 2 classes différentes");

      const params = {};
      modelDef.params.forEach(p => { params[p.key] = getParam(p.key); });
      if (params.maxNodes === 0) params.maxNodes = null;

      let data;
      if (multiDate && isGee && periods.length >= 2) {
        // ── Mode multi-dates ───────────────────────────────────────────
        const body = {
          layer_id:     selLayerId,
          gee_params:   selLayer?._geeParams || null,
          aoi:          aoiPoly,
          rois,
          model:        modelId,
          model_params: params,
          train_ratio:  trainRatio,
          class_colors: classes.map(c => c.color),
          date_periods: periods,
        };
        const resp = await fetch(`${API}/api/gee/classify/multidate`, {
          method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body),
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({ detail:"Erreur serveur" }));
          throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        data = await resp.json();
        setActivePeriodIdx(0);
        addResultLayer(data, uniqueId("classif"),
          `Multi-dates [${periods.map(p=>p.label).join("→")}] — ${selLayer?.name||""}`);
      } else {
        // ── Mode date unique (existant) ────────────────────────────────
        const body = {
          layer_id:    selLayerId,
          layer_type:  isGee ? "gee" : "tile",
          gee_params:  selLayer?._geeParams || null,
          tile_url:    selLayer?.tileUrl    || null,
          aoi:         aoiPoly,
          rois,
          model:        modelId,
          model_params: params,
          train_ratio:  trainRatio,
          class_colors: classes.map(c => c.color),
        };
        const resp = await fetch(`${API}/api/gee/classify`, {
          method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body),
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({ detail:"Erreur serveur" }));
          throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        data = await resp.json();
        addResultLayer(data, uniqueId("classif"), `Classif. ${selLayer?.name||""} — ${modelDef.label}`);
      }
      // Enrichit le résultat avec l'ID du modèle pour l'onglet "Modèle" du modal
      setResult({ ...data, classifier_id: modelId });
      setRestyleColors((data.legend || []).map(l => l.color));
      setStep(4);
    } catch(e) { setError(e.message); } finally { setLoading(false); }
  };

  // ── Classification auto GEE ───────────────────────────────────────────────
  const runAuto = async () => {
    if (!aoiPoly) { setAutoError("Définissez d'abord la zone AOI"); return; }
    setAutoLoading(true); setAutoError(null); setResult(null);
    try {
      const selAuto    = autoList.find(a => a.id === autoId);
      const classColors = selAuto?.classes?.map(c => c.color) || [];
      const needsDates  = selAuto?.needs_dates !== false;

      if (multiDate && needsDates && periods.length >= 2) {
        // ── Multi-dates : un appel auto-classify par période ──────────
        const periodResults = [];
        for (let pi = 0; pi < periods.length; pi++) {
          const p = periods[pi];
          setActivePeriodIdx(pi);
          const body = {
            classifier_id: autoId, aoi: aoiPoly,
            date_start: p.start, date_end: p.end,
            class_colors: classColors,
          };
          const resp = await fetch(`${API}/api/gee/auto-classify`, {
            method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body),
          });
          if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail:"Erreur" }));
            throw new Error(`[${p.label}] ${err.detail || `HTTP ${resp.status}`}`);
          }
          const d = await resp.json();
          periodResults.push({
            label: p.label, start: p.start, end: p.end,
            tile_url:        d.tile_url,
            legend:          d.legend,
            image_bounds:    d.image_bounds,
            cloud_cover_pct: d.cloud_cover_pct ?? null,
            image_count:     d.image_count     ?? null,
          });
        }
        const combined = {
          backend:          "gee_multidate",
          tile_url:         periodResults[0].tile_url,
          image_url:        null,
          image_bounds:     periodResults[0].image_bounds,
          legend:           periodResults[0].legend,
          bands_used:       selAuto ? [selAuto.band || "label"] : [],
          class_labels:     (periodResults[0].legend || []).map(l => l.label),
          period_results:   periodResults,
          classifier_label: selAuto?.label,
          metrics: null, confusion_matrix: null, feature_importance: null, job_id: null,
        };
        setResult(combined);
        setRestyleColors((combined.legend || []).map(l => l.color));
        setActivePeriodIdx(0);
        setAutoStep(2);
        addResultLayer(combined, uniqueId("auto"),
          `Multi-dates [${periods.map(p=>p.label).join("→")}] — ${selAuto?.label||autoId}`);
      } else {
        // ── Date unique (existant) ────────────────────────────────────
        const body = {
          classifier_id: autoId, aoi: aoiPoly,
          date_start: autoDateStart, date_end: autoDateEnd,
          class_colors: classColors,
        };
        const resp = await fetch(`${API}/api/gee/auto-classify`, {
          method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body),
        });
        if (!resp.ok) {
          const err = await resp.json().catch(() => ({ detail:"Erreur" }));
          throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const data = await resp.json();
        setResult(data);
        setRestyleColors((data.legend || []).map(l => l.color));
        setAutoStep(2);
        addResultLayer(data, uniqueId("auto"), `Auto: ${selAuto?.label || autoId}`);
      }
    } catch(e) { setAutoError(e.message); } finally { setAutoLoading(false); }
  };

  // ── Clustering non supervisé ─────────────────────────────────────────────
  const runCluster = async () => {
    if (!aoiPoly)    { setClusterError("Définissez la zone AOI"); return; }
    if (!selLayerId) { setClusterError("Sélectionnez une couche GEE"); return; }
    if (!isGee)      { setClusterError("Le clustering nécessite une couche GEE"); return; }
    setClusterLoading(true); setClusterError(null); setResult(null);
    try {
      const body = {
        gee_params:   selLayer._geeParams,
        aoi:          aoiPoly,
        method:       clusterMethod,
        n_clusters:   nClusters,
        max_clusters: maxClusters,
        sample_size:  5000,
        class_colors: null,
      };
      const resp = await fetch(`${API}/api/gee/cluster`, {
        method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body),
      });
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail:"Erreur" }));
        throw new Error(err.detail || `HTTP ${resp.status}`);
      }
      const data = await resp.json();
      setResult(data);
      setRestyleColors((data.legend || []).map(l => l.color));
      setClusterStep(2);
      addResultLayer(data, uniqueId("cluster"),
        `Cluster ${clusterMethod === "xmeans" ? "X-Means" : `K-Means k=${nClusters}`} — ${selLayer?.name || ""}`);
    } catch(e) { setClusterError(e.message); } finally { setClusterLoading(false); }
  };

  // ── Restyle ──────────────────────────────────────────────────────────────
  const applyRestyle = async () => {
    if (!result?.job_id) return;
    setRestyleLoading(true);
    try {
      const body = { job_id:result.job_id, class_colors:restyleColors };
      const resp = await fetch(`${API}/api/gee/classify/restyle`, {
        method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();

      // Mettre à jour la légende dans le state
      setResult(prev => ({
        ...prev,
        legend:(prev.legend || []).map((l, i) => ({...l, color:restyleColors[i] || l.color})),
      }));

      // Mettre à jour le source MapLibre
      const map = getMap();
      const src = currentLayerId.current;
      if (map && src) {
        if (data.tile_url) {
          // GEE : reconstruire la source raster
          const lyrId = `${src}-lyr`;
          try { if (map.getLayer(lyrId)) map.removeLayer(lyrId); } catch(_) {}
          try { if (map.getSource(src))  map.removeSource(src);  } catch(_) {}
          try {
            map.addSource(src, { type:"raster", tiles:[data.tile_url], tileSize:256 });
            map.addLayer({ id:lyrId, type:"raster", source:src, paint:{"raster-opacity":0.85} });
          } catch(ex) { console.warn("restyle GEE:", ex); }
        } else if (data.image_url && currentLayerBounds.current) {
          // WMS : updateImage
          const [W, S, E, N] = currentLayerBounds.current;
          try {
            const imgSrc = map.getSource(src);
            if (imgSrc?.updateImage) {
              imgSrc.updateImage({ url:data.image_url, coordinates:[[W,N],[E,N],[E,S],[W,S]] });
            }
          } catch(ex) { console.warn("restyle WMS:", ex); }
        }
      }
    } catch(e) { console.warn("restyle error:", e); } finally { setRestyleLoading(false); }
  };

  // ── Styles communs ────────────────────────────────────────────────────────
  const S = {
    section: { display:"flex", flexDirection:"column", gap:8 },
    label:   { fontSize:10, fontWeight:500, color:C.dim,
               textTransform:"uppercase", letterSpacing:"0.05em", marginBottom:2 },
    inp:     { fontFamily:F, fontSize:11, padding:"7px 10px", borderRadius:7,
               border:`1px solid ${C.bdr}`, background:C.input, color:C.txt,
               width:"100%", outline:"none", boxSizing:"border-box" },
    btn: (accent) => ({
      fontFamily:F, fontSize:11, padding:"7px 12px", borderRadius:8,
      border:`1px solid ${accent ? "transparent" : C.bdr}`,
      background: accent ? "#22a558" : C.hover,
      color:      accent ? "#fff"    : C.txt,
      cursor:"pointer", display:"flex", alignItems:"center", gap:5, flexShrink:0,
    }),
    card: { background:C.card, borderRadius:10, padding:10, border:`1px solid ${C.bdr}` },
    colorPick: {
      width:22, height:22, padding:0, border:`2px solid ${C.bdr}`,
      borderRadius:4, cursor:"pointer", flexShrink:0, background:"none",
    },
  };

  // ── Section AOI réutilisable ──────────────────────────────────────────────
  const renderAoi = () => (
    <div>
      <div style={S.label}>Zone d'intérêt (AOI)</div>
      {!aoiPoly && !aoiDrawing && (
        <div style={{ display:"flex", gap:6 }}>
          <button style={{ ...S.btn(true), flex:1, justifyContent:"center", gap:5 }}
                  onClick={startAoi}
                  disabled={mode === "supervised" && !selLayerId}>
            <IcEdit size={13}/> Dessiner
          </button>
          <button style={{ ...S.btn(false), flex:1, justifyContent:"center", gap:5 }}
                  onClick={() => aoiFileRef.current?.click()}
                  title="Importer un fichier GeoJSON comme AOI">
            <IcFolder size={13}/> Importer fichier
          </button>
          <input ref={aoiFileRef} type="file" accept=".geojson,.json"
                 style={{ display:"none" }} onChange={handleAoiFile}/>
        </div>
      )}
      {aoiDrawing && (
        <div style={S.card}>
          <p style={{ fontSize:11, color:C.txt, margin:"0 0 6px", display:"flex", alignItems:"center", gap:5 }}>
            <IcMouse size={13}/> Cliquez sur la carte — {aoiPoints.length} sommet(s)
          </p>
          {!isGee && aoiPoints.length >= 3 && (
            <p style={{ fontSize:11, margin:"0 0 6px", display:"flex", alignItems:"center", gap:4,
                        color: aoiArea > 1 ? "#e41a1c" : "#4daf4a" }}>
              {aoiArea <= 1 ? <IcCheck size={12}/> : <IcAlert size={12}/>} {aoiArea.toFixed(3)} km²
              {aoiArea > 1 ? " (max 10 km²)" : " / 1 km²"}
            </p>
          )}
          <div style={{ display:"flex", gap:6 }}>
            <button style={{ ...S.btn(true), gap:5 }} onClick={closeAoi} disabled={aoiPoints.length < 3}>
              <IcCheck size={13}/> Valider
            </button>
            <button style={S.btn(false)} onClick={resetAoi}>Annuler</button>
          </div>
        </div>
      )}
      {aoiPoly && (
        <div style={{ ...S.card, display:"flex", alignItems:"center", justifyContent:"space-between" }}>
          <span style={{ fontSize:11, color:"#4daf4a", display:"inline-flex", alignItems:"center", gap:5 }}>
            <IcCheck size={12}/> Zone définie{!isGee ? ` — ${aoiArea.toFixed(3)} km²` : ""}
          </span>
          <button style={{ ...S.btn(false), padding:"4px 8px" }} onClick={resetAoi}><IcTrash size={12}/></button>
        </div>
      )}
    </div>
  );

  // ── Panneau résultats (partagé supervisé / auto) ──────────────────────────
  const renderResults = (isSupervised) => (
    <div style={S.section}>

      {/* ── Bandeau Landsat fallback ── */}
      {result.landsat_fallback && (
        <div style={{
          padding:"8px 10px", borderRadius:6,
          background:"#e07b0011", border:"1px solid #e07b0044",
          fontSize:10, color:"#e07b00",
        }}>
          <IcSatellite size={12} style={{ verticalAlign:"middle" }}/> <strong>Bascule automatique vers {result.sensor || "Landsat"}</strong>
          {" "}— {result.fallback_reason || "Sentinel-2 non disponible sur cette période."}
          <div style={{ fontSize:9, marginTop:3, color:"#e07b00", opacity:0.85 }}>
            Bandes utilisées : Blue, Green, Red, NIR, SWIR1, SWIR2, NDVI, NDWI, NDBI
          </div>
        </div>
      )}

      {/* ── Sélecteur de période (multi-dates uniquement) ── */}
      {result.backend === "gee_multidate" && result.period_results?.length > 0 && (
        <div style={S.card}>
          <div style={{ fontSize:9, color:C.dim, textTransform:"uppercase",
                        letterSpacing:".05em", marginBottom:6, display:"flex", alignItems:"center", gap:5 }}>
            <IcCalendar size={11}/> Afficher sur la carte
          </div>
          <div style={{ display:"flex", gap:4, flexWrap:"wrap" }}>
            {result.period_results.map((pr, i) => (
              <button key={i} onClick={() => switchPeriodOnMap(i)} style={{
                ...S.btn(i === activePeriodIdx),
                padding:"5px 10px", fontSize:10,
                background: i === activePeriodIdx ? "#22a558" : C.hover,
                color:      i === activePeriodIdx ? "#fff"    : C.txt,
              }}>
                {pr.label}
              </button>
            ))}
          </div>
          {result.period_results[activePeriodIdx]?.cloud_cover_pct != null && (
            <div style={{ fontSize:9, color:C.dim, marginTop:5, display:"flex", alignItems:"center", gap:4 }}>
              <IcCloud size={11}/> {result.period_results[activePeriodIdx].cloud_cover_pct}% nuages ·{" "}
              {result.period_results[activePeriodIdx].image_count} image(s)
            </div>
          )}
        </div>
      )}

      {/* Bannière accuracy */}
      <div style={{ ...S.card, background:"#4daf4a11", border:"1px solid #4daf4a44", textAlign:"center" }}>
        {result.metrics ? (<>
          <div style={{ fontSize:24, fontWeight:700, color:"#4daf4a" }}>
            {Math.round((result.metrics.overall_accuracy || 0) * 100)}%
          </div>
          <div style={{ fontSize:11, color:C.dim }}>Accuracy globale</div>
          <div style={{ fontSize:10, color:C.dim, marginTop:2 }}>
            Kappa {result.metrics.kappa?.toFixed(3) || "—"} ·{" "}
            {result.backend === "gee" ? "GEE natif" : "sklearn local"}
          </div>
        </>) : (
          <div style={{ fontSize:11, color:C.dim, display:"flex", alignItems:"center", justifyContent:"center", gap:5 }}>
            <IcCheck size={12}/> Classification terminée — métriques non disponibles
          </div>
        )}
      </div>

      {/* Résumé classes + F1 scores */}
      {result.metrics?.per_class?.length > 0 && (
        <div style={{ display:"flex", flexDirection:"column", gap:3 }}>
          <div style={{ ...S.label }}>Classes</div>
          {result.metrics.per_class.map((pc, i) => (
            <div key={pc.class_id ?? i} style={{
              display:"flex", alignItems:"center", gap:8,
              padding:"4px 8px", borderRadius:6,
              background:C.hover, border:`0.5px solid ${C.bdr}`,
            }}>
              <div style={{
                width:10, height:10, borderRadius:2, flexShrink:0,
                background:(result.legend?.[i]?.color || "#888"),
                border:`0.5px solid ${C.bdr}`,
              }}/>
              <span style={{ fontSize:11, color:C.txt, flex:1 }}>{pc.label}</span>
              <span style={{ fontSize:10, color:C.dim, fontFamily:"monospace" }}>
                F1 {Math.round((pc.f1 || 0) * 100)}%
              </span>
            </div>
          ))}
          <div style={{ fontSize:9, color:C.dim, marginTop:2, display:"flex", alignItems:"center", gap:5 }}>
            <IcBulb size={11}/> Couleurs éditables dans le Gestionnaire de couches
          </div>
        </div>
      )}

      {/* Bandes utilisées */}
      {result.bands_used?.length > 0 && (
        <p style={{ fontSize:10, color:C.dim, margin:0 }}>
          Bandes : {result.bands_used.join(", ")}
        </p>
      )}

      <p style={{ fontSize:11, color:"#4daf4a", margin:0, display:"flex", alignItems:"center", gap:5 }}>
        <IcCheck size={12}/> Couche classifiée ajoutée à la carte
      </p>

      <button style={{ ...S.btn(result.metrics ? true : false), justifyContent:"center", gap:6 }}
              onClick={() => setShowMetrics(true)}>
        {result.metrics ? <><IcBarChart size={13}/> Voir les métriques détaillées</> : <><IcInfo size={13}/> Informations sur la classification</>}
      </button>

      <button style={{ ...S.btn(false), justifyContent:"center" }} onClick={() => {
        setResult(null); setAoiPoly(null); setAoiPoints([]);
        if (isSupervised) {
          setStep(1); setClasses([]); setError(null);
        } else if (mode === "auto") {
          setAutoStep(1); setAutoError(null);
        } else {
          setClusterStep(1); setClusterError(null);
        }
      }}>
        {mode === "cluster" ? <span style={{ display:"inline-flex", alignItems:"center", gap:5 }}><IcRefreshCw size={13}/> Nouveau clustering</span> : "Nouvelle classification"}
      </button>
    </div>
  );

  // ─────────────────────────────────────────────────────────────────────────
  return (
    <>
      <div style={{ flex:1, minHeight:0, overflowY:"auto", overflowX:"hidden",
                    display:"flex", flexDirection:"column" }}>
        <div style={{ padding:12, display:"flex", flexDirection:"column", gap:10 }}>

          {/* ── Tabs Mode ────────────────────────────────────────────────── */}
          <div style={{ display:"flex", gap:3, background:C.hover, borderRadius:8, padding:3 }}>
            {[["supervised",IcClassif,"Supervisée"], ["auto",IcZap,"Auto GEE"], ["cluster",IcCircleDot,"Non sup."]].map(([m, Icon, lbl]) => (
              <button key={m} onClick={() => {
                setMode(m); setError(null); setAutoError(null); setClusterError(null);
                setResult(null); setStep(1); setAutoStep(1); setClusterStep(1);
              }} style={{
                flex:1, fontFamily:F, fontSize:10, padding:"5px 2px", borderRadius:6,
                border:"none", cursor:"pointer",
                background: mode===m ? C.card : "transparent",
                color:      mode===m ? C.txt  : C.dim,
                fontWeight: mode===m ? 600    : 400,
                display:"inline-flex", alignItems:"center", justifyContent:"center", gap:4,
              }}><Icon size={12}/> {lbl}</button>
            ))}
          </div>

          {/* ════════════ MODE SUPERVISÉ ════════════════════════════════════ */}
          {mode === "supervised" && (
            <>
              <Stepper step={step} C={C}/>

              {/* ── Étape 1 : Couche + AOI ──────────────────────────── */}
              {step === 1 && (
                <div style={S.section}>
                  <div>
                    <div style={S.label}>Couche raster source</div>
                    {rasterLayers.length === 0 ? (
                      <p style={{ fontSize:11, color:C.dim, margin:0 }}>
                        Aucune couche raster chargée
                      </p>
                    ) : (
                      <select value={selLayerId} onChange={e => setSelLayerId(e.target.value)} style={S.inp}>
                        <option value="">— Sélectionner —</option>
                        {rasterLayers.map(l => (
                          <option key={l.id} value={l.id}>
                            {l.name} {l._geeParams ? "(GEE)" : "(WMS)"}
                          </option>
                        ))}
                      </select>
                    )}
                    {selLayerId && (
                      <div style={{ marginTop:5, fontSize:10 }}>
                        <span style={{
                          background: isGee ? "#4A90D922" : "#e07b0022",
                          color:      isGee ? "#4A90D9"   : "#e07b00",
                          padding:"2px 8px", borderRadius:20,
                          display:"inline-flex", alignItems:"center", gap:5,
                        }}>
                          {isGee ? <><IcSatellite size={11}/> GEE — sans limite de taille</> : <><IcMap size={11}/> WMS/Tile — max 10 km²</>}
                        </span>
                      </div>
                    )}
                  </div>

                  {renderAoi()}

                  {error && <p style={{ color:"#e41a1c", fontSize:11, margin:0 }}>{error}</p>}

                  <button style={{ ...S.btn(true), justifyContent:"center" }}
                          onClick={() => { setStep(2); setError(null); }}
                          disabled={!canGoStep2}>
                    Suivant →
                  </button>
                </div>
              )}

              {/* ── Étape 2 : ROIs / Labels ──────────────────────────── */}
              {step === 2 && (
                <div style={S.section}>
                  <div style={{ display:"flex", gap:4, background:C.hover, borderRadius:7, padding:3 }}>
                    {[["manual",IcEdit,"ROI manuels"], ["layer",IcFolder,"Couche vecteur"]].map(([m, Icon, lbl]) => (
                      <button key={m} onClick={() => setRoiMode(m)} style={{
                        flex:1, fontFamily:F, fontSize:11, padding:"4px", borderRadius:5,
                        border:"none", cursor:"pointer",
                        background: roiMode===m ? C.card : "transparent",
                        color:      roiMode===m ? C.txt  : C.dim,
                        fontWeight: roiMode===m ? 600    : 400,
                        display:"inline-flex", alignItems:"center", justifyContent:"center", gap:5,
                      }}><Icon size={12}/> {lbl}</button>
                    ))}
                  </div>

                  {roiMode === "manual" && (
                    <>
                      {classes.map(cls => (
                        <div key={cls.id} style={S.card}>
                          {/* En-tête classe : color picker + nom + supprimer */}
                          <div style={{ display:"flex", alignItems:"center", gap:6, marginBottom:6 }}>
                            <input type="color" value={cls.color}
                                   onChange={e => setClassColor(cls.id, e.target.value)}
                                   style={S.colorPick} title="Couleur de la classe"/>
                            <input value={cls.label}
                                   onChange={e => renameClass(cls.id, e.target.value)}
                                   style={{ ...S.inp, flex:1 }} placeholder="Nom de classe"/>
                            <button style={{ ...S.btn(false), padding:"3px 7px" }}
                                    onClick={() => removeClass(cls.id)}><IcTrash size={12}/></button>
                          </div>

                          {/* Tags ROIs */}
                          {cls.rois.length > 0 && (
                            <div style={{ display:"flex", flexWrap:"wrap", gap:4, marginBottom:6 }}>
                              {cls.rois.map((_, i) => (
                                <span key={i} style={{
                                  fontSize:10, padding:"2px 8px", borderRadius:20,
                                  background: cls.color + "22", color:cls.color,
                                  display:"flex", alignItems:"center", gap:4,
                                }}>
                                  Poly {i+1}
                                  <button onClick={() => removeRoi(cls.id, i)}
                                          style={{ background:"none", border:"none",
                                                   cursor:"pointer", color:cls.color, padding:0, display:"flex" }}><IcX size={11}/></button>
                                </span>
                              ))}
                            </div>
                          )}

                          {drawingFor === cls.id ? (
                            <div style={{ display:"flex", flexDirection:"column", gap:4 }}>
                              <p style={{ fontSize:10, color:C.dim, margin:0, display:"flex", alignItems:"center", gap:5 }}>
                                <IcMouse size={12}/> {curPts.length} sommet(s) — cliquez sur la carte
                              </p>
                              <div style={{ display:"flex", gap:4 }}>
                                <button style={{ ...S.btn(true), gap:5 }} onClick={closeRoi} disabled={curPts.length < 3}>
                                  <IcCheck size={12}/> Valider polygone
                                </button>
                                <button style={S.btn(false)} onClick={cancelRoi}>Annuler</button>
                              </div>
                            </div>
                          ) : (
                            <button style={{ ...S.btn(false), width:"100%", justifyContent:"center" }}
                                    onClick={() => startRoi(cls.id)}
                                    disabled={drawingFor !== null && drawingFor !== cls.id}>
                              + Dessiner un polygone
                            </button>
                          )}
                        </div>
                      ))}

                      <button style={{ ...S.btn(false), justifyContent:"center" }} onClick={addClass}>
                        + Ajouter une classe
                      </button>

                      {classes.length >= 2 && (
                        <p style={{ fontSize:10, color:C.dim, margin:0 }}>
                          {classes.filter(c => c.rois.length > 0).length}/{classes.length} classes
                          avec polygones
                        </p>
                      )}
                    </>
                  )}

                  {roiMode === "layer" && (
                    <>
                      <div>
                        <div style={S.label}>Couche vecteur de référence</div>
                        <select value={vecLayerId} onChange={e => setVecLayerId(e.target.value)} style={S.inp}>
                          <option value="">— Sélectionner —</option>
                          {vectorLayers.map(l => <option key={l.id} value={l.id}>{l.name}</option>)}
                        </select>
                      </div>
                      {vecLayerId && (() => {
                        const vl = layers.find(l => l.id === vecLayerId);
                        const attrs = Object.keys(vl?.geojson?.features?.[0]?.properties || {});
                        return (
                          <div>
                            <div style={S.label}>Attribut de classe</div>
                            <select value={vecAttr} onChange={e => setVecAttr(e.target.value)} style={S.inp}>
                              <option value="">— Sélectionner —</option>
                              {attrs.map(a => <option key={a} value={a}>{a}</option>)}
                            </select>
                          </div>
                        );
                      })()}
                      {classes.length > 0 && (
                        <div style={S.card}>
                          <div style={{ ...S.label, marginBottom:8 }}>
                            Classes détectées ({classes.length}) — couleurs modifiables
                          </div>
                          <div style={{ display:"flex", flexDirection:"column", gap:6 }}>
                            {classes.map(cls => (
                              <div key={cls.id} style={{ display:"flex", alignItems:"center", gap:8 }}>
                                <input type="color" value={cls.color}
                                       onChange={e => setClassColor(cls.id, e.target.value)}
                                       style={S.colorPick}/>
                                <span style={{ fontSize:11, color:C.txt }}>{cls.label}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </>
                  )}

                  {error && <p style={{ color:"#e41a1c", fontSize:11, margin:0 }}>{error}</p>}

                  <div style={{ display:"flex", gap:6 }}>
                    <button style={S.btn(false)} onClick={() => setStep(1)}>← Retour</button>
                    <button style={{ ...S.btn(true), flex:1, justifyContent:"center" }}
                            onClick={() => { setStep(3); setError(null); }}
                            disabled={!canGoStep3}>
                      Suivant →
                    </button>
                  </div>
                </div>
              )}

              {/* ── Étape 3 : Modèle ─────────────────────────────────── */}
              {step === 3 && (
                <div style={S.section}>
                  <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:6 }}>
                    {MODELS.map(m => (
                      <button key={m.id} onClick={() => setModelId(m.id)} style={{
                        fontFamily:F, border:"none", cursor:"pointer",
                        borderRadius:8, padding:"8px 6px", textAlign:"center",
                        background: modelId===m.id ? "#4A90D922" : C.hover,
                        outline:    modelId===m.id ? "2px solid #4A90D9" : "none",
                        color: C.txt,
                      }}>
                        <div style={{ display:"flex", justifyContent:"center" }}>{m.icon && <m.icon size={16}/>}</div>
                        <div style={{ fontSize:10, fontWeight:600, marginTop:2 }}>{m.label}</div>
                        <div style={{ fontSize:9, color:C.dim, marginTop:1 }}>{m.desc}</div>
                      </button>
                    ))}
                  </div>

                  <div style={S.card}>
                    <div style={{ fontSize:11, fontWeight:600, color:C.txt, marginBottom:8 }}>
                      Paramètres — {modelDef.label}
                    </div>
                    {modelDef.params.map(param => (
                      <div key={param.key} style={{ marginBottom:8 }}>
                        <div style={{ ...S.label, marginBottom:3 }}>
                          {param.label}
                          {param.hint && <span style={{ color:C.dim }}> ({param.hint})</span>}
                        </div>
                        {param.type === "select" ? (
                          <select value={getParam(param.key)}
                                  onChange={e => setParam(param.key, e.target.value)}
                                  style={S.inp}>
                            {param.options.map(o => <option key={o} value={o}>{o}</option>)}
                          </select>
                        ) : (
                          <input type="number" value={getParam(param.key)}
                                 min={param.min} max={param.max} step={param.step || 1}
                                 onChange={e => setParam(param.key, parseFloat(e.target.value))}
                                 style={S.inp}/>
                        )}
                      </div>
                    ))}
                  </div>

                  <div>
                    <div style={{ ...S.label, display:"flex", justifyContent:"space-between" }}>
                      <span>Ratio entraînement / test</span>
                      <span style={{ color:C.txt, textTransform:"none", letterSpacing:0 }}>
                        {Math.round(trainRatio*100)}% / {Math.round((1-trainRatio)*100)}%
                      </span>
                    </div>
                    <input type="range" min={50} max={90} step={5}
                           value={Math.round(trainRatio*100)}
                           onChange={e => setTrainRatio(parseInt(e.target.value)/100)}
                           style={{ width:"100%", accentColor:"#4A90D9" }}/>
                  </div>

                  {error && <p style={{ color:"#e41a1c", fontSize:11, margin:0 }}>{error}</p>}

                  <div style={{ display:"flex", gap:6 }}>
                    <button style={S.btn(false)} onClick={() => setStep(2)}>← Retour</button>
                    <button style={{ ...S.btn(true), flex:1, justifyContent:"center",
                                     opacity:loading ? 0.6 : 1 }}
                            onClick={run} disabled={loading}>
                      {loading ? "Calcul en cours…" : <><IcRocket size={13}/> Lancer la classification</>}
                    </button>
                  </div>
                </div>
              )}

              {/* ── Étape 4 : Résultats ──────────────────────────────── */}
              {step === 4 && result && renderResults(true)}
            </>
          )}

          {/* ════════════ MODE AUTO GEE ═════════════════════════════════════ */}
          {mode === "auto" && (
            <>
              {autoStep === 1 && (
                <div style={S.section}>
                  {/* Sélection classifieur */}
                  <div>
                    <div style={S.label}>Classifieur pré-entraîné GEE</div>
                    {autoList.length === 0 ? (
                      <p style={{ fontSize:11, color:C.dim, margin:0 }}>Chargement des classifieurs…</p>
                    ) : (
                      <div style={{ display:"flex", flexDirection:"column", gap:6 }}>
                        {autoList.map(a => (
                          <button key={a.id} onClick={() => setAutoId(a.id)} style={{
                            fontFamily:F, fontSize:11, padding:"8px 10px", borderRadius:8,
                            border:"none", cursor:"pointer", textAlign:"left",
                            background: autoId===a.id ? "#4A90D922" : C.hover,
                            outline:    autoId===a.id ? "2px solid #4A90D9" : "none",
                            color: C.txt,
                          }}>
                            <div style={{ fontWeight:600, marginBottom:2 }}>{a.label}</div>
                            <div style={{ fontSize:10, color:C.dim }}>{a.description}</div>
                            {/* Badge de disponibilité temporelle */}
                            {(a.date_min || a.date_max) && (
                              <div style={{
                                fontSize:9, marginTop:4, padding:"2px 6px", borderRadius:4,
                                background:"#4A90D911", color:"#4A90D9",
                                display:"inline-flex", alignItems:"center", gap:4,
                              }}>
                                <IcCalendar size={10}/> {a.date_min ? a.date_min.slice(0,7) : "…"} →{" "}
                                {a.date_max ? a.date_max.slice(0,7) : "aujourd'hui"}
                              </div>
                            )}
                            {a.avail_note && !a.date_min && !a.date_max && (
                              <div style={{ fontSize:9, color:C.dim, marginTop:3, fontStyle:"italic" }}>
                                {a.avail_note}
                              </div>
                            )}
                            {a.classes && (
                              <div style={{ display:"flex", flexWrap:"wrap", gap:3, marginTop:5 }}>
                                {a.classes.slice(0, 8).map(c => (
                                  <span key={c.class_id} style={{
                                    fontSize:9, padding:"1px 6px", borderRadius:10,
                                    background: c.color + "33", color:c.color,
                                  }}>{c.label}</span>
                                ))}
                                {a.classes.length > 8 && (
                                  <span style={{ fontSize:9, color:C.dim }}>+{a.classes.length-8}</span>
                                )}
                              </div>
                            )}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>

                  {renderAoi()}

                  {/* Plage de dates / multi-dates */}
                  {autoList.find(a => a.id === autoId)?.needs_dates !== false && (
                    <>
                      {/* Toggle */}
                      <div style={{
                        padding:"8px 10px", borderRadius:8,
                        background: multiDate ? "#4A90D911" : C.hover,
                        border: `0.5px solid ${multiDate ? "#4A90D944" : C.bdr}`,
                      }}>
                        <label style={{ display:"flex", alignItems:"center", gap:8, cursor:"pointer" }}>
                          <input type="checkbox" checked={multiDate}
                                 onChange={e => setMultiDate(e.target.checked)}
                                 style={{ width:14, height:14, cursor:"pointer" }}/>
                          <span style={{ fontSize:11, fontWeight:600,
                                         color: multiDate ? "#4A90D9" : C.txt, display:"inline-flex", alignItems:"center", gap:5 }}>
                            <IcCalendar size={12}/> Comparaison multi-dates
                          </span>
                        </label>
                      </div>

                      {/* Date unique */}
                      {!multiDate && (
                        <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:8 }}>
                          <div>
                            <div style={S.label}>Date début</div>
                            <input type="date" value={autoDateStart}
                                   onChange={e => setAutoDateStart(e.target.value)} style={S.inp}/>
                          </div>
                          <div>
                            <div style={S.label}>Date fin</div>
                            <input type="date" value={autoDateEnd}
                                   onChange={e => setAutoDateEnd(e.target.value)} style={S.inp}/>
                          </div>
                        </div>
                      )}

                      {/* Périodes */}
                      {multiDate && (
                        <div style={{ display:"flex", flexDirection:"column", gap:5 }}>
                          <div style={{ fontSize:9, color:C.dim,
                                        textTransform:"uppercase", letterSpacing:".05em" }}>
                            Périodes à comparer
                          </div>
                          {periods.map((p, i) => (
                            <div key={i} style={{ display:"flex", gap:4, alignItems:"center" }}>
                              <input value={p.label}
                                     onChange={e => updatePeriod(i, "label", e.target.value)}
                                     placeholder={`P${i+1}`}
                                     style={{ ...S.inp, width:48, textAlign:"center", padding:"5px 4px" }}/>
                              <input type="date" value={p.start}
                                     onChange={e => updatePeriod(i, "start", e.target.value)}
                                     style={{ ...S.inp, flex:1, padding:"5px 6px" }}/>
                              <input type="date" value={p.end}
                                     onChange={e => updatePeriod(i, "end", e.target.value)}
                                     style={{ ...S.inp, flex:1, padding:"5px 6px" }}/>
                              {periods.length > 2 && (
                                <button style={{ ...S.btn(false), padding:"4px 6px" }}
                                        onClick={() => removePeriod(i)}><IcTrash size={12}/></button>
                              )}
                            </div>
                          ))}
                          <button style={{ ...S.btn(false), justifyContent:"center", fontSize:10 }}
                                  onClick={addPeriod}>
                            + Ajouter période
                          </button>
                        </div>
                      )}
                    </>
                  )}

                  {autoError && (
                    <div style={{
                      padding:"8px 10px", borderRadius:6,
                      background:"#e41a1c11", border:"1px solid #e41a1c44",
                      fontSize:11, color:"#e41a1c", whiteSpace:"pre-line",
                    }}>
                      {autoError}
                    </div>
                  )}

                  <button style={{ ...S.btn(true), justifyContent:"center",
                                   opacity:autoLoading ? 0.6 : 1 }}
                          onClick={runAuto}
                          disabled={autoLoading || !aoiPoly || !autoId}>
                    {autoLoading
                      ? `${multiDate ? `Période ${activePeriodIdx+1}/${periods.length}…` : "Calcul GEE en cours…"}`
                      : multiDate ? <><IcCalendar size={13}/> Comparer {periods.length} périodes</> : <><IcZap size={13}/> Lancer la classification auto</>}
                  </button>

                  <p style={{ fontSize:10, color:C.dim, margin:0, textAlign:"center" }}>
                    Sans restriction de taille · Résultat en quelques secondes
                  </p>
                </div>
              )}

              {autoStep === 2 && result && renderResults(false)}
            </>
          )}

          {/* ════════════ MODE CLUSTERING ═══════════════════════════════════ */}
          {mode === "cluster" && (
            <>
              {clusterStep === 1 && (
                <div style={S.section}>
                  {/* Couche source (GEE obligatoire) */}
                  <div>
                    <div style={S.label}>Couche GEE source</div>
                    {rasterLayers.filter(l => l._geeParams).length === 0 ? (
                      <p style={{ fontSize:11, color:C.dim, margin:0 }}>
                        Aucune couche GEE — chargez d'abord une couche depuis le panneau GEE
                      </p>
                    ) : (
                      <select value={selLayerId} onChange={e => setSelLayerId(e.target.value)} style={S.inp}>
                        <option value="">— Sélectionner —</option>
                        {rasterLayers.filter(l => l._geeParams).map(l => (
                          <option key={l.id} value={l.id}>{l.name} (GEE)</option>
                        ))}
                      </select>
                    )}
                  </div>

                  {renderAoi()}

                  {/* Méthode */}
                  <div>
                    <div style={S.label}>Méthode de clustering</div>
                    <div style={{ display:"flex", gap:6 }}>
                      {[["kmeans","K-Means","Nombre fixe de clusters"],
                        ["xmeans","X-Means","Nombre automatique"]].map(([m,lbl,desc]) => (
                        <button key={m} onClick={() => setClusterMethod(m)} style={{
                          flex:1, fontFamily:F, fontSize:11, padding:"8px 6px",
                          borderRadius:8, border:"none", cursor:"pointer", textAlign:"center",
                          background: clusterMethod===m ? "#4A90D922" : C.hover,
                          outline:    clusterMethod===m ? "2px solid #4A90D9" : "none",
                          color: C.txt,
                        }}>
                          <div style={{ fontWeight:600 }}>{lbl}</div>
                          <div style={{ fontSize:9, color:C.dim, marginTop:2 }}>{desc}</div>
                        </button>
                      ))}
                    </div>
                  </div>

                  {/* Paramètres */}
                  {clusterMethod === "kmeans" ? (
                    <div>
                      <div style={{ ...S.label, display:"flex", justifyContent:"space-between" }}>
                        <span>Nombre de clusters</span>
                        <span style={{ color:C.txt, textTransform:"none", letterSpacing:0 }}>{nClusters}</span>
                      </div>
                      <input type="range" min={2} max={15} step={1} value={nClusters}
                             onChange={e => setNClusters(parseInt(e.target.value))}
                             style={{ width:"100%", accentColor:"#4A90D9" }}/>
                    </div>
                  ) : (
                    <div>
                      <div style={{ ...S.label, display:"flex", justifyContent:"space-between" }}>
                        <span>Nb max de clusters (X-Means)</span>
                        <span style={{ color:C.txt, textTransform:"none", letterSpacing:0 }}>{maxClusters}</span>
                      </div>
                      <input type="range" min={3} max={20} step={1} value={maxClusters}
                             onChange={e => setMaxClusters(parseInt(e.target.value))}
                             style={{ width:"100%", accentColor:"#4A90D9" }}/>
                    </div>
                  )}

                  {clusterError && <p style={{ color:"#e41a1c", fontSize:11, margin:0 }}>{clusterError}</p>}

                  <button style={{ ...S.btn(true), justifyContent:"center",
                                   opacity:clusterLoading ? 0.6 : 1 }}
                          onClick={runCluster}
                          disabled={clusterLoading || !aoiPoly || !selLayerId}>
                    {clusterLoading ? "Clustering GEE en cours…" : <><IcCircleDot size={13}/> Lancer le clustering</>}
                  </button>

                  <p style={{ fontSize:10, color:C.dim, margin:0, textAlign:"center" }}>
                    Sans restriction de taille · K-Means et X-Means GEE natifs
                  </p>
                </div>
              )}

              {clusterStep === 2 && result && renderResults(false)}
            </>
          )}

        </div>
      </div>

      {/* Modal métriques */}
      {showMetrics && result && (
        <ClassifMetricsModal result={result} onClose={() => setShowMetrics(false)}/>
      )}
    </>
  );
}
