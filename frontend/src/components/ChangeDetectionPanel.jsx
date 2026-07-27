/**
 * ChangeDetectionPanel.jsx — Détection de changement GEE
 * Compare deux périodes sur le même indice et zone.
 * Résultat : carte différentielle colorée + stats de changement.
 *
 * Datasets supportés : Sentinel-2, Landsat 8/9, MODIS NDVI/LST, Sentinel-1
 * Indices : NDVI, NDWI, NDBI, EVI, LST, VV (SAR)
 * Sortie : tuile WMS différentielle + histogramme des changements
 */

import { useState, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";
import { IcSatellite, IcLeaf, IcThermometer, IcSearch, IcCheck, IcCloud, IcMap, IcHexagon } from "../icons";

// ── Datasets compatibles détection de changement ─────────────
const CD_DATASETS = [
  { id:"sentinel2",  label:"Sentinel-2",     desc:"10m · 2017–présent",  icon:IcSatellite, cloud:true,
    indices:["NDVI","NDWI","NDBI","EVI","RGB"] },
  { id:"landsat8",   label:"Landsat 8",       desc:"30m · 2013–présent",  icon:IcSatellite, cloud:true,
    indices:["NDVI","NDWI","LST"] },
  { id:"landsat9",   label:"Landsat 9",       desc:"30m · 2021–présent",  icon:IcSatellite, cloud:true,
    indices:["NDVI","NDWI","LST"] },
  { id:"modis_ndvi", label:"MODIS NDVI",      desc:"500m · 2000–présent", icon:IcLeaf, cloud:false,
    indices:["NDVI","EVI"] },
  { id:"modis_lst",  label:"MODIS LST",       desc:"1km · 2000–présent",  icon:IcThermometer, cloud:false,
    indices:["LST Jour","LST Nuit"] },
  { id:"sentinel1",  label:"Sentinel-1 SAR",  desc:"10m · 2014–présent",  icon:IcSatellite, cloud:false,
    indices:["VV","VH"] },
];

// ── Palettes différentielles ──────────────────────────────────
// Rouge = perte/diminution, Blanc = stable, Vert/Bleu = gain/augmentation
const DIFF_PALETTES = {
  NDVI:       { palette:["#a50026","#d73027","#f46d43","#fdae61","#ffffff","#a6d96a","#66bd63","#1a9850","#006837"], min:-0.4, max:0.4, unit:"ΔNDVI" },
  NDWI:       { palette:["#8B4513","#DEB887","#ffffff","#AED6F1","#1A5276"], min:-0.4, max:0.4, unit:"ΔNDWI" },
  NDBI:       { palette:["#1a9850","#fee08b","#d73027"], min:-0.4, max:0.4, unit:"ΔNDBI" },
  EVI:        { palette:["#a50026","#fdae61","#ffffff","#a6d96a","#006837"], min:-0.4, max:0.4, unit:"ΔEVI" },
  LST:        { palette:["#2166ac","#92c5de","#ffffff","#f4a582","#b2182b"], min:-5,   max:5,   unit:"ΔTemp (°C)" },
  "LST Jour": { palette:["#2166ac","#92c5de","#ffffff","#f4a582","#b2182b"], min:-5,   max:5,   unit:"ΔTemp (°C)" },
  "LST Nuit": { palette:["#2166ac","#92c5de","#ffffff","#f4a582","#b2182b"], min:-5,   max:5,   unit:"ΔTemp (°C)" },
  VV:         { palette:["#d73027","#fee08b","#ffffff","#91bfdb","#4575b4"], min:-5,   max:5,   unit:"ΔVV (dB)" },
  VH:         { palette:["#d73027","#fee08b","#ffffff","#91bfdb","#4575b4"], min:-5,   max:5,   unit:"ΔVH (dB)" },
  RGB:        { palette:["#d73027","#fee08b","#ffffff","#91bfdb","#4575b4"], min:-1000,max:1000, unit:"Δ Réflectance" },
};

// ── Séparateur ────────────────────────────────────────────────
function Sep({ children }) {
  const C = useThemeContext();
  return (
    <div style={{
      fontSize:9, color:C.dim, textTransform:"uppercase",
      letterSpacing:".06em", fontWeight:600,
      borderBottom:`0.5px solid ${C.bdr}`, paddingBottom:4, marginTop:2,
    }}>{children}</div>
  );
}

// ── Carte mini légende gradient ───────────────────────────────
function DiffLegend({ palette, min, max, unit }) {
  const C = useThemeContext();
  if (!palette?.length) return null;
  const colors   = palette.map(c => c.startsWith("#") ? c : `#${c}`);
  const gradient = `linear-gradient(to right, ${colors.join(",")})`;
  const mid      = (min + max) / 2;
  const fmt      = v => Number.isInteger(v) ? v : v.toFixed(2);
  return (
    <div>
      <div style={{ height:8, borderRadius:4, background:gradient, margin:"3px 0 2px" }}/>
      <div style={{ display:"flex", justifyContent:"space-between", fontSize:9, color:C.dim, fontFamily:M }}>
        <span>{fmt(min)}</span>
        <span style={{ color:C.txt, fontWeight:600 }}>{unit}</span>
        <span>{fmt(max)}</span>
      </div>
      <div style={{ display:"flex", justifyContent:"space-between", fontSize:8, color:C.dim }}>
        <span>Perte / Diminution</span>
        <span>Gain / Augmentation</span>
      </div>
    </div>
  );
}

// ── Mini histogramme SVG des changements ─────────────────────
function ChangeHistogram({ stats }) {
  const C = useThemeContext();
  if (!stats) return null;
  const { pct_gain, pct_loss, pct_stable, mean_change, std_change } = stats;
  const total = 100;
  const W = 280, H = 16;

  return (
    <div style={{ display:"flex", flexDirection:"column", gap:5 }}>
      {/* Barre empilée gain/stable/perte */}
      <div style={{ display:"flex", borderRadius:4, overflow:"hidden", height:16 }}>
        <div title={`Perte: ${pct_loss.toFixed(1)}%`}
          style={{ width:`${pct_loss}%`, background:"#d73027", minWidth: pct_loss > 0 ? 2 : 0 }}/>
        <div title={`Stable: ${pct_stable.toFixed(1)}%`}
          style={{ width:`${pct_stable}%`, background:C.bdr }}/>
        <div title={`Gain: ${pct_gain.toFixed(1)}%`}
          style={{ width:`${pct_gain}%`, background:"#1a9850", minWidth: pct_gain > 0 ? 2 : 0 }}/>
      </div>
      {/* Légende */}
      <div style={{ display:"flex", gap:10, fontSize:9 }}>
        <div style={{ display:"flex", alignItems:"center", gap:4 }}>
          <div style={{ width:8, height:8, borderRadius:2, background:"#d73027" }}/>
          <span style={{ color:C.dim }}>Perte <span style={{ color:C.red, fontFamily:M, fontWeight:600 }}>{pct_loss.toFixed(1)}%</span></span>
        </div>
        <div style={{ display:"flex", alignItems:"center", gap:4 }}>
          <div style={{ width:8, height:8, borderRadius:2, background:C.bdr }}/>
          <span style={{ color:C.dim }}>Stable <span style={{ color:C.txt, fontFamily:M }}>{pct_stable.toFixed(1)}%</span></span>
        </div>
        <div style={{ display:"flex", alignItems:"center", gap:4 }}>
          <div style={{ width:8, height:8, borderRadius:2, background:"#1a9850" }}/>
          <span style={{ color:C.dim }}>Gain <span style={{ color:C.acc, fontFamily:M, fontWeight:600 }}>{pct_gain.toFixed(1)}%</span></span>
        </div>
      </div>
      {/* Stats numériques */}
      <div style={{ display:"flex", gap:6 }}>
        {[
          ["Δ moyen", mean_change?.toFixed(3)],
          ["Écart-type", std_change?.toFixed(3)],
          ["Surface gain", stats.area_gain_km2 ? `${stats.area_gain_km2.toFixed(1)} km²` : "–"],
          ["Surface perte", stats.area_loss_km2 ? `${stats.area_loss_km2.toFixed(1)} km²` : "–"],
        ].map(([lbl, val]) => (
          <div key={lbl} style={{
            flex:1, background:C.hover, borderRadius:5, padding:"5px 6px",
            border:`0.5px solid ${C.bdr}`,
          }}>
            <div style={{ fontSize:8, color:C.dim, textTransform:"uppercase" }}>{lbl}</div>
            <div style={{ fontSize:11, fontWeight:600, color:C.txt, fontFamily:M }}>{val ?? "–"}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
export default function ChangeDetectionPanel({ mapRef, layers=[], onAddRasterLayer }) {
  const C = useThemeContext();

  const [datasetId, setDatasetId] = useState("sentinel2");
  const [index,     setIndex]     = useState("NDVI");
  const [cloudMax,  setCloudMax]  = useState(20);

  // Période A (référence / avant)
  const [dateStartA, setDateStartA] = useState("2018-06-01");
  const [dateEndA,   setDateEndA]   = useState("2018-09-30");

  // Période B (comparaison / après)
  const [dateStartB, setDateStartB] = useState("2024-06-01");
  const [dateEndB,   setDateEndB]   = useState("2024-09-30");

  // Seuil de détection (pixels au-delà → considérés comme changement)
  const [threshold, setThreshold] = useState(0.1);

  // Composite
  const [composite, setComposite] = useState("median");

  // ROI
  const [roiMode,    setRoiMode]    = useState("bbox");
  const [roiLayerId, setRoiLayerId] = useState("");

  // Résultat
  const [loading,  setLoading]  = useState(false);
  const [error,    setError]    = useState(null);
  const [result,   setResult]   = useState(null);  // { tile_url, vis_params, stats, name }
  const [status,   setStatus]   = useState(null);

  const ds      = CD_DATASETS.find(d => d.id === datasetId);
  const palette = DIFF_PALETTES[index] || DIFF_PALETTES.NDVI;

  const handleDataset = (id) => {
    const d = CD_DATASETS.find(x => x.id === id);
    setDatasetId(id);
    setIndex(d?.indices[0] || "NDVI");
    setResult(null); setError(null); setStatus(null);
  };

  // ── ROI ─────────────────────────────────────────────────────
  const getRoi = useCallback(() => {
    if (roiMode === "layer" && roiLayerId) {
      const layer = layers.find(l => l.id === roiLayerId);
      if (layer?.geojson?.features?.length) {
        const polys = layer.geojson.features.filter(
          f => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
        );
        if (polys.length > 0) {
          const geom = polys.length === 1
            ? polys[0].geometry
            : { type:"GeometryCollection", geometries:polys.map(f => f.geometry) };
          return { roi_geojson:geom, bbox:null };
        }
      }
    }
    try {
      const map = mapRef.current?.getMap?.();
      if (map) {
        const canvas = map.getCanvas();
        const sw = map.unproject([0, canvas.offsetHeight]);
        const ne = map.unproject([canvas.offsetWidth, 0]);
        return { roi_geojson:null, bbox:[sw.lng, sw.lat, ne.lng, ne.lat] };
      }
    } catch(_) {}
    return { roi_geojson:null, bbox:null };
  }, [roiMode, roiLayerId, layers, mapRef]);

  // ── Calcul ──────────────────────────────────────────────────
  const compute = useCallback(async () => {
    setLoading(true); setError(null); setResult(null); setStatus(null);
    const { bbox, roi_geojson } = getRoi();

    try {
      const res = await fetch(`${API}/gee/change-detection`, {
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({
          dataset:     datasetId,
          index,
          date_start_a: dateStartA,
          date_end_a:   dateEndA,
          date_start_b: dateStartB,
          date_end_b:   dateEndB,
          cloud_max:    cloudMax,
          composite,
          threshold,
          bbox,
          roi_geojson,
        }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `Erreur ${res.status}`);
      }
      const data = await res.json();
      setResult(data);
      setStatus({ type:"ok", msg:`${data.name}` });

      // Ajouter la couche différentielle sur la carte
      const sourceId = `gee_change_${datasetId}_${index}_${Date.now()}`;
      onAddRasterLayer?.({
        id:          sourceId,
        mapSourceId: sourceId,
        mapLayerId:  `${sourceId}-layer`,
        name:        data.name,
        type:        "wms",
        tileUrl:     data.tile_url,
        opacity:     0.85,
        bbox:        data.clip_bbox || null,
        visParams:   data.vis_params || null,
      });
    } catch(e) {
      setError(e.message);
      setStatus({ type:"error", msg:e.message });
    }
    setLoading(false);
  }, [datasetId, index, dateStartA, dateEndA, dateStartB, dateEndB,
      cloudMax, composite, threshold, getRoi, onAddRasterLayer]);

  const polyLayers = layers.filter(l =>
    !l.isRaster && l.geojson?.features?.some(
      f => f.geometry?.type==="Polygon" || f.geometry?.type==="MultiPolygon"
    )
  );

  const inp = {
    fontFamily:M, fontSize:10, padding:"4px 7px", borderRadius:5,
    background:C.input, color:C.txt, border:`0.5px solid ${C.bdr}`,
    outline:"none", width:"100%", boxSizing:"border-box",
  };

  const statColor = { ok:C.acc, error:C.red };

  // ══════════════════════════════════════════════════════════════
  return (
    <div style={{ display:"flex", flexDirection:"column", width:"100%", height:"100%", minHeight:0, overflow:"hidden" }}>

      {/* Header */}
      <div style={{
        padding:"5px 12px", borderBottom:`0.5px solid ${C.bdr}`,
        fontSize:10, color:C.dim, flexShrink:0,
        display:"flex", alignItems:"center", gap:6,
      }}>
        <IcSearch size={12}/>
        <span>Détection de changement · GEE</span>
      </div>

      <div style={{ flex:1, minHeight:0, overflowY:"auto", overflowX:"hidden", padding:"12px 14px", display:"flex", flexDirection:"column", gap:10 }}>

        {/* ── Dataset ────────────────────────────────────────── */}
        <Sep>Satellite / Collection</Sep>
        <div style={{ display:"flex", flexDirection:"column", gap:2 }}>
          {CD_DATASETS.map(d => (
            <div key={d.id} onClick={() => handleDataset(d.id)}
              style={{
                display:"flex", alignItems:"center", gap:8,
                padding:"4px 8px", borderRadius:5, cursor:"pointer",
                background: datasetId===d.id ? C.acc+"18" : "transparent",
                border: `0.5px solid ${datasetId===d.id ? C.acc+"55" : "transparent"}`,
              }}>
              <span style={{ display:"flex", color:datasetId===d.id?C.acc:C.mut }}>{d.icon && <d.icon size={14}/>}</span>
              <div style={{ flex:1 }}>
                <div style={{ fontSize:11, fontWeight:datasetId===d.id?600:400, color:datasetId===d.id?C.acc:C.txt }}>{d.label}</div>
                <div style={{ fontSize:9, color:C.dim }}>{d.desc}</div>
              </div>
              {datasetId===d.id && <IcCheck size={12} color={C.acc}/>}
            </div>
          ))}
        </div>

        {/* ── Indice ─────────────────────────────────────────── */}
        <Sep>Indice à comparer</Sep>
        <div style={{ display:"flex", flexWrap:"wrap", gap:4 }}>
          {(ds?.indices || []).map(idx => (
            <button key={idx} onClick={() => setIndex(idx)} style={{
              fontFamily:F, fontSize:10, padding:"4px 10px", borderRadius:5,
              background: index===idx ? C.acc+"18" : "transparent",
              border: `0.5px solid ${index===idx ? C.acc+"66" : C.bdr}`,
              color: index===idx ? C.acc : C.dim,
              cursor:"pointer", fontWeight:index===idx?600:400,
            }}>{idx}</button>
          ))}
        </div>

        {/* ── Période A — Référence ──────────────────────────── */}
        <Sep>Période A — Référence (avant)</Sep>
        <div style={{ display:"flex", gap:6 }}>
          <div style={{ flex:1 }}>
            <div style={{ fontSize:9, color:C.dim, marginBottom:2 }}>Début</div>
            <input type="date" value={dateStartA} onChange={e=>setDateStartA(e.target.value)} style={inp}/>
          </div>
          <div style={{ flex:1 }}>
            <div style={{ fontSize:9, color:C.dim, marginBottom:2 }}>Fin</div>
            <input type="date" value={dateEndA} onChange={e=>setDateEndA(e.target.value)} style={inp}/>
          </div>
        </div>

        {/* ── Période B — Comparaison ────────────────────────── */}
        <Sep>Période B — Comparaison (après)</Sep>
        <div style={{ display:"flex", gap:6 }}>
          <div style={{ flex:1 }}>
            <div style={{ fontSize:9, color:C.dim, marginBottom:2 }}>Début</div>
            <input type="date" value={dateStartB} onChange={e=>setDateStartB(e.target.value)} style={inp}/>
          </div>
          <div style={{ flex:1 }}>
            <div style={{ fontSize:9, color:C.dim, marginBottom:2 }}>Fin</div>
            <input type="date" value={dateEndB} onChange={e=>setDateEndB(e.target.value)} style={inp}/>
          </div>
        </div>

        {/* Résumé visuel A → B */}
        <div style={{
          display:"flex", alignItems:"center", gap:6, padding:"6px 10px",
          background:C.hover, borderRadius:6, border:`0.5px solid ${C.bdr}`,
          fontSize:10,
        }}>
          <span style={{ color:C.blu, fontFamily:M, fontWeight:600 }}>{dateStartA.slice(0,7)}</span>
          <span style={{ color:C.dim }}>→</span>
          <span style={{ color:C.blu, fontFamily:M, fontWeight:600 }}>{dateEndA.slice(0,7)}</span>
          <span style={{ color:C.dim, margin:"0 6px" }}>vs</span>
          <span style={{ color:C.acc, fontFamily:M, fontWeight:600 }}>{dateStartB.slice(0,7)}</span>
          <span style={{ color:C.dim }}>→</span>
          <span style={{ color:C.acc, fontFamily:M, fontWeight:600 }}>{dateEndB.slice(0,7)}</span>
        </div>

        {/* ── Composite ──────────────────────────────────────── */}
        <Sep>Composite par période</Sep>
        <div style={{ display:"flex", gap:4 }}>
          {[["least_cloudy","Moins nuageux"],["median","Médiane"],["mosaic","Mosaïque"]].map(([k,lbl]) => (
            <button key={k} onClick={() => setComposite(k)} style={{
              fontFamily:F, fontSize:9, flex:1, padding:"5px 0", borderRadius:5, cursor:"pointer",
              background: composite===k ? C.acc+"18" : "transparent",
              border: `0.5px solid ${composite===k ? C.acc+"55" : C.bdr}`,
              color: composite===k ? C.acc : C.dim, fontWeight:composite===k?600:400,
            }}>{lbl}</button>
          ))}
        </div>

        {/* ── Filtre nuages ──────────────────────────────────── */}
        {ds?.cloud && (
          <div style={{ display:"flex", alignItems:"center", gap:8 }}>
            <span style={{ fontSize:9, color:C.dim, flexShrink:0, display:"inline-flex", alignItems:"center", gap:4 }}><IcCloud size={11}/> Max nuages</span>
            <input type="range" min={0} max={100} step={5} value={cloudMax}
              onChange={e=>setCloudMax(parseInt(e.target.value))} style={{ flex:1, height:3 }}/>
            <span style={{ fontFamily:M, fontSize:10, color:C.txt, flexShrink:0 }}>{cloudMax}%</span>
          </div>
        )}

        {/* ── Seuil de détection ─────────────────────────────── */}
        <Sep>Seuil de détection</Sep>
        <div style={{ display:"flex", alignItems:"center", gap:8 }}>
          <span style={{ fontSize:9, color:C.dim, flexShrink:0 }}>Δ min</span>
          <input type="range" min={0.01} max={0.5} step={0.01} value={threshold}
            onChange={e=>setThreshold(parseFloat(e.target.value))} style={{ flex:1, height:3 }}/>
          <span style={{ fontFamily:M, fontSize:10, color:C.txt, flexShrink:0, minWidth:32 }}>±{threshold.toFixed(2)}</span>
        </div>
        <div style={{ fontSize:9, color:C.dim }}>
          Pixels avec |ΔIndice| &gt; {threshold.toFixed(2)} → considérés comme changement
        </div>

        {/* ── Zone d'analyse ─────────────────────────────────── */}
        <Sep>Zone d'analyse</Sep>
        <div style={{ display:"flex", gap:4 }}>
          {[["bbox",IcMap,"Vue carte"],["layer",IcHexagon,"Couche (mask)"]].map(([m,Icon,lbl]) => (
            <button key={m} onClick={() => setRoiMode(m)} style={{
              fontFamily:F, flex:1, fontSize:10, padding:"5px 0", borderRadius:5, cursor:"pointer",
              background: roiMode===m ? C.acc+"18" : "transparent",
              border: `0.5px solid ${roiMode===m ? C.acc+"55" : C.bdr}`,
              color: roiMode===m ? C.acc : C.dim,
              display:"inline-flex", alignItems:"center", justifyContent:"center", gap:5,
            }}><Icon size={12}/> {lbl}</button>
          ))}
        </div>
        {roiMode === "bbox" && (
          <div style={{ fontSize:9, color:C.dim }}>Zoomez sur la zone avant de calculer.</div>
        )}
        {roiMode === "layer" && (
          polyLayers.length === 0
            ? <div style={{ fontSize:9, color:C.amb, padding:"5px 8px", background:C.amb+"12", borderRadius:5 }}>
                Aucune couche polygone disponible.
              </div>
            : <select value={roiLayerId} onChange={e=>setRoiLayerId(e.target.value)}
                style={{ fontFamily:F, fontSize:10, padding:"5px 8px", borderRadius:5, background:C.input, color:C.txt, border:`0.5px solid ${roiLayerId?C.acc+"66":C.bdr}`, outline:"none", width:"100%" }}>
                <option value="">— Choisir une couche —</option>
                {polyLayers.map(l => <option key={l.id} value={l.id}>{l.name} ({l.featureCount})</option>)}
              </select>
        )}

        {/* ── Status ─────────────────────────────────────────── */}
        {status && (
          <div style={{
            fontSize:10, padding:"5px 8px", borderRadius:5,
            background: statColor[status.type]+"15",
            border: `0.5px solid ${statColor[status.type]}44`,
            color: statColor[status.type],
          }}>{status.msg}</div>
        )}

        {/* ── Résultat : légende + histogramme ───────────────── */}
        {result && (
          <>
            <Sep>Carte différentielle — {index}</Sep>
            <DiffLegend
              palette={result.vis_params?.palette || palette.palette}
              min={result.vis_params?.min ?? palette.min}
              max={result.vis_params?.max ?? palette.max}
              unit={palette.unit}
            />
            {result.stats && (
              <>
                <Sep>Statistiques de changement</Sep>
                <ChangeHistogram stats={result.stats} />
              </>
            )}
          </>
        )}

        {/* ── Bouton calculer ────────────────────────────────── */}
        <div style={{ marginTop:"auto", paddingTop:6 }}>
          <button onClick={compute} disabled={loading} style={{
            fontFamily:F, fontSize:12, fontWeight:600,
            padding:"10px 0", borderRadius:7, width:"100%",
            background: loading ? C.hover : C.acc,
            color: loading ? C.dim : "#fff",
            border:"none", cursor:loading?"default":"pointer",
            opacity: loading ? 0.7 : 1,
            display:"flex", alignItems:"center", justifyContent:"center", gap:7,
          }}>
            <IcSearch size={14}/> {loading ? "Calcul GEE en cours…" : "Détecter les changements"}
          </button>
          <div style={{ fontSize:9, color:C.dim, textAlign:"center", marginTop:4 }}>
            Calcule B − A pixel par pixel · résultat ajouté comme couche
          </div>
        </div>

        <div style={{ height:8 }}/>
      </div>
    </div>
  );
}
