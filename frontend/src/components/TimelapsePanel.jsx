/**
 * TimelapsePanel.jsx — Module Timelapse GEE v3
 * Tout à plat, rien de caché, scroll natif comme GEEPanel / SpatialPanel.
 */

import { useState, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";
import { IcSatellite, IcLeaf, IcThermometer, IcCloud, IcFilm, IcCheck, IcCloud as IcCloudy,
  IcMap, IcHexagon, IcAlert, IcFileDown, IcImage, IcCalendar, IcSave } from "../icons";

const TL_DATASETS = [
  {
    id: "landsat",
    label: "Landsat",
    desc: "30m · 1984 – présent (5/7/8/9 auto)",
    icon: IcSatellite,
    has_cloud: true,
    indices: ["RGB", "NDVI", "NDWI", "LST"],
    year_min: 1984,
    year_max: 2025,
  },
  {
    id: "sentinel2",
    label: "Sentinel-2",
    desc: "10m · 2017 – présent",
    icon: IcSatellite,
    has_cloud: true,
    indices: ["RGB", "NDVI", "NDWI", "NDBI", "False Color"],
    year_min: 2017,
    year_max: 2025,
  },
  {
    id: "modis_ndvi",
    label: "MODIS NDVI",
    desc: "500m · 2000 – présent",
    icon: IcLeaf,
    has_cloud: false,
    indices: ["NDVI", "EVI"],
    year_min: 2000,
    year_max: 2025,
  },
  {
    id: "modis_lst",
    label: "MODIS LST",
    desc: "1km · 2000 – présent · température de surface",
    icon: IcThermometer,
    has_cloud: false,
    indices: ["LST Jour", "LST Nuit"],
    year_min: 2000,
    year_max: 2025,
  },
  {
    id: "era5",
    label: "ERA5 Climat",
    desc: "11km · 1940 – présent · mensuel",
    icon: IcCloud,
    has_cloud: false,
    indices: ["Température", "Précipitations"],
    year_min: 1940,
    year_max: 2025,
  },
];

const FREQ_OPTIONS = [
  { id: "annual",   label: "Annuel",     desc: "1 image / an" },
  { id: "seasonal", label: "Saisonnier", desc: "1 image / saison" },
  { id: "monthly",  label: "Mensuel",    desc: "1 image / mois" },
  { id: "biweekly", label: "Bimensuel",  desc: "1 image / 2 semaines" },
];

const COMPOSITE_OPTIONS = [
  { id: "least_cloudy", label: "Moins nuageux" },
  { id: "median",       label: "Médiane" },
  { id: "mosaic",       label: "Mosaïque" },
];

const MONTHS_FR = ["Jan","Fév","Mar","Avr","Mai","Juin","Juil","Août","Sep","Oct","Nov","Déc"];

// ── Séparateur de section ─────────────────────────────────────
function SepTitle({ children }) {
  const C = useThemeContext();
  return (
    <div style={{
      fontSize: 9, color: C.dim, textTransform: "uppercase",
      letterSpacing: ".06em", fontWeight: 600,
      borderBottom: `0.5px solid ${C.bdr}`, paddingBottom: 4, marginTop: 4,
    }}>
      {children}
    </div>
  );
}

// ── Sélecteur plage de mois ───────────────────────────────────
function MonthRange({ startMonth, endMonth, onChange }) {
  const C = useThemeContext();
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 2 }}>
        {MONTHS_FR.map((m, i) => {
          const idx = i + 1;
          const inRange = idx >= startMonth && idx <= endMonth;
          return (
            <button key={idx}
              onClick={() => {
                if (idx < startMonth) onChange(idx, endMonth);
                else if (idx > endMonth) onChange(startMonth, idx);
                else if (idx === startMonth && idx !== endMonth) onChange(idx + 1, endMonth);
                else if (idx === endMonth && idx !== startMonth) onChange(startMonth, idx - 1);
                else onChange(idx, idx);
              }}
              style={{
                fontFamily: M, fontSize: 9, padding: "3px 5px", borderRadius: 3,
                background: inRange ? C.acc + "25" : "transparent",
                border: `0.5px solid ${inRange ? C.acc + "55" : C.bdr}`,
                color: inRange ? C.acc : C.dim,
                cursor: "pointer", minWidth: 28, textAlign: "center",
              }}
            >{m}</button>
          );
        })}
      </div>
      <div style={{ fontSize: 9, color: C.dim }}>
        {MONTHS_FR[startMonth - 1]} → {MONTHS_FR[endMonth - 1]}
      </div>
    </div>
  );
}

// ── Toggle on/off ─────────────────────────────────────────────
function Toggle({ val, set, label }) {
  const C = useThemeContext();
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <button onClick={() => set(v => !v)} style={{
        fontFamily: F, fontSize: 9, padding: "3px 8px", borderRadius: 4, flexShrink: 0,
        background: val ? C.acc + "18" : "transparent",
        border: `0.5px solid ${val ? C.acc + "55" : C.bdr}`,
        color: val ? C.acc : C.dim, cursor: "pointer", minWidth: 40,
        display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 4,
      }}>{val ? <><IcCheck size={11}/> On</> : "Off"}</button>
      <span style={{ fontSize: 10, color: C.mut }}>{label}</span>
    </div>
  );
}

// ── Aperçu GIF ────────────────────────────────────────────────
function GifPreview({ url, status, onDownload }) {
  const C = useThemeContext();
  const [loaded, setLoaded] = useState(false);
  return (
    <div style={{
      borderRadius: 8, padding: 10, border: `0.5px solid ${C.acc}44`,
      background: C.hover, display: "flex", flexDirection: "column", gap: 8,
    }}>
      <div style={{ fontSize: 11, fontWeight: 600, color: C.acc, display: "flex", alignItems: "center", gap: 6 }}><IcFilm size={13}/> Timelapse généré</div>
      <div style={{
        borderRadius: 6, overflow: "hidden", background: C.input,
        border: `0.5px solid ${C.bdr}`, minHeight: 120,
        display: "flex", alignItems: "center", justifyContent: "center", position: "relative",
      }}>
        {!loaded && <div style={{ fontSize: 10, color: C.dim, position: "absolute" }}>Chargement…</div>}
        <img src={url} alt="Timelapse" onLoad={() => setLoaded(true)}
          style={{ width: "100%", height: "auto", display: loaded ? "block" : "none", borderRadius: 6 }} />
      </div>
      {status && (
        <div style={{ fontSize: 10, color: C.mut, lineHeight: 1.6 }}>
          {status.frames && <span style={{ display: "inline-flex", alignItems: "center", gap: 3 }}><IcImage size={11}/> {status.frames} frames · </span>}
          {status.period && <span style={{ display: "inline-flex", alignItems: "center", gap: 3 }}><IcCalendar size={11}/> {status.period} · </span>}
          {status.size_mb && <span style={{ display: "inline-flex", alignItems: "center", gap: 3 }}><IcSave size={11}/> {status.size_mb} MB</span>}
          {status.source && <div style={{ fontSize: 9, color: C.dim, marginTop: 2 }}>Source : {status.source}</div>}
        </div>
      )}
      <button onClick={onDownload} style={{
        fontFamily: F, fontSize: 11, fontWeight: 500, padding: "7px 0",
        borderRadius: 6, width: "100%",
        background: C.acc + "18", color: C.acc,
        border: `0.5px solid ${C.acc}55`, cursor: "pointer",
        display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
      }}><IcFileDown size={13}/> Télécharger le GIF</button>
    </div>
  );
}

// ════════════════════════════════════════════════════════════════
export default function TimelapsePanel({ mapRef, layers = [], sidebarWidth = 0, chatWidth = 0 }) {
  const C = useThemeContext();

  const [datasetId,  setDatasetId]  = useState("landsat");
  const [index,      setIndex]      = useState("RGB");

  const dsObj = TL_DATASETS.find(d => d.id === datasetId);

  const [yearStart,   setYearStart]   = useState(2000);
  const [yearEnd,     setYearEnd]     = useState(2024);
  const [monthStart,  setMonthStart]  = useState(1);
  const [monthEnd,    setMonthEnd]    = useState(12);
  const [freq,        setFreq]        = useState("annual");
  const [composite,   setComposite]   = useState("least_cloudy");
  const [cloudMax,    setCloudMax]    = useState(30);

  const [roiMode,    setRoiMode]    = useState("bbox");
  const [roiLayerId, setRoiLayerId] = useState("");

  const [fps,      setFps]      = useState(3);
  const [gifWidth, setGifWidth] = useState(512);

  const [annTitle,     setAnnTitle]     = useState("");
  const [annDesc,      setAnnDesc]      = useState("");
  const [annCredits,   setAnnCredits]   = useState("");  // suffix optionnel, le prefix OpenMapAgents · GEE est fixe
  const [showNorth,    setShowNorth]    = useState(true);
  const [showScale,    setShowScale]    = useState(true);
  const [showProgress, setShowProgress] = useState(true);
  const [showLegend,   setShowLegend]   = useState(true);

  const [loading,   setLoading]   = useState(false);
  const [progress,  setProgress]  = useState(null);
  const [error,     setError]     = useState(null);
  const [gifResult, setGifResult] = useState(null);

  // ── Dataset ─────────────────────────────────────────────────
  const handleDataset = (id) => {
    const d = TL_DATASETS.find(x => x.id === id);
    setDatasetId(id);
    setIndex(d?.indices[0] || "RGB");
    setYearStart(prev => Math.max(d?.year_min || 1984, Math.min(prev, d?.year_max || 2025)));
    setYearEnd(prev => Math.min(d?.year_max || 2025, Math.max(prev, d?.year_min || 1984)));
    setGifResult(null);
    setError(null);
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
            : { type: "GeometryCollection", geometries: polys.map(f => f.geometry) };
          return { roi_geojson: geom, bbox: null };
        }
      }
    }
    try {
      const map = mapRef.current?.getMap?.();
      if (map) {
        const canvas = map.getCanvas();
        const sw = map.unproject([sidebarWidth, canvas.offsetHeight]);
        const ne = map.unproject([canvas.offsetWidth - chatWidth, 0]);
        return { roi_geojson: null, bbox: [sw.lng, sw.lat, ne.lng, ne.lat] };
      }
    } catch (_) {}
    return { roi_geojson: null, bbox: null };
  }, [roiMode, roiLayerId, layers, mapRef, sidebarWidth, chatWidth]);

  const estimateFrames = () => {
    const years = Math.max(1, yearEnd - yearStart + 1);
    const mpy   = monthEnd - monthStart + 1;
    if (freq === "annual")   return years;
    if (freq === "seasonal") return years * Math.ceil(mpy / 3);
    if (freq === "monthly")  return years * mpy;
    return years * mpy * 2;
  };

  // ── Génération ──────────────────────────────────────────────
  const generate = useCallback(async () => {
    setLoading(true);
    setError(null);
    setGifResult(null);
    setProgress("Envoi de la requête GEE…");
    const { bbox, roi_geojson } = getRoi();
    const payload = {
      dataset:          datasetId,
      index,
      year_start:       yearStart,
      year_end:         yearEnd,
      month_start:      monthStart,
      month_end:        monthEnd,
      frequency:        freq,
      composite,
      cloud_max:        cloudMax,
      fps,
      gif_width:        gifWidth,
      ann_title:        annTitle.trim(),
      ann_desc:         annDesc.trim(),
      ann_credits:      annCredits.trim(),
      show_north:       showNorth,
      show_scale:       showScale,
      show_progress:    showProgress,
      show_legend:      showLegend,
      timestamp_format: "readable",
      bbox,
      roi_geojson,
    };
    try {
      setProgress("Calcul GEE en cours — 30 à 120 sec…");
      const res = await fetch(`${API}/gee/timelapse`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `Erreur ${res.status}`);
      }
      const data = await res.json();
      setGifResult({
        url: data.gif_url,
        status: { frames: data.frames, period: data.period, size_mb: data.size_mb, source: data.source },
      });
      setProgress(null);
    } catch (e) {
      setError(e.message);
      setProgress(null);
    }
    setLoading(false);
  }, [
    datasetId, index, yearStart, yearEnd, monthStart, monthEnd,
    freq, composite, cloudMax, fps, gifWidth,
    annTitle, annDesc, annCredits,
    showNorth, showScale, showProgress, showLegend, getRoi,
  ]);

  const download = () => {
    if (!gifResult?.url) return;
    const a = document.createElement("a");
    a.href = gifResult.url;
    a.download = `timelapse_${datasetId}_${yearStart}-${yearEnd}_${index}.gif`;
    a.click();
  };

  // ── Styles communs ──────────────────────────────────────────
  const lbl = { fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", marginBottom: 3 };
  const inp = {
    fontFamily: M, fontSize: 10, padding: "5px 7px", borderRadius: 5,
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };

  const polyLayers = layers.filter(l =>
    !l.isRaster && l.geojson?.features?.some(
      f => f.geometry?.type === "Polygon" || f.geometry?.type === "MultiPolygon"
    )
  );
  const est = estimateFrames();

  // ════════════════════════════════════════════════════════════════
  return (
    <div style={{ display: "flex", flexDirection: "column", width: "100%", height: "100%", minHeight: 0, overflow: "hidden" }}>

      {/* ── Status bar ─────────────────────────────────────── */}
      <div style={{
        padding: "5px 12px", borderBottom: `0.5px solid ${C.bdr}`,
        fontSize: 10, color: C.dim, flexShrink: 0,
        display: "flex", alignItems: "center", gap: 6,
      }}>
        <IcFilm size={12}/><span>Timelapse GIF · Google Earth Engine</span>
      </div>

      {/* ── Contenu scrollable ─────────────────────────────── */}
      <div style={{
        flex: 1, minHeight: 0,
        overflowY: "auto", overflowX: "hidden",
        padding: "12px 14px",
        display: "flex", flexDirection: "column", gap: 12,
      }}>

        {/* ─── SATELLITE ───────────────────────────────────── */}
        <SepTitle>Satellite / Collection</SepTitle>
        <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
          {TL_DATASETS.map(d => (
            <div key={d.id} onClick={() => handleDataset(d.id)}
              style={{
                display: "flex", alignItems: "center", gap: 8,
                padding: "5px 8px", borderRadius: 5, cursor: "pointer",
                background: datasetId === d.id ? C.acc + "18" : "transparent",
                border: `0.5px solid ${datasetId === d.id ? C.acc + "55" : "transparent"}`,
              }}>
              <span style={{ display: "flex", color: datasetId === d.id ? C.acc : C.mut }}>{d.icon && <d.icon size={15}/>}</span>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 11, fontWeight: datasetId === d.id ? 600 : 400, color: datasetId === d.id ? C.acc : C.txt }}>
                  {d.label}
                </div>
                <div style={{ fontSize: 9, color: C.dim }}>{d.desc}</div>
              </div>
              {datasetId === d.id && <IcCheck size={12} color={C.acc}/>}
            </div>
          ))}
        </div>

        {/* ─── INDICE ──────────────────────────────────────── */}
        <SepTitle>Produit / Indice</SepTitle>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
          {(dsObj?.indices || []).map(idx => (
            <button key={idx} onClick={() => setIndex(idx)} style={{
              fontFamily: F, fontSize: 10, padding: "4px 10px", borderRadius: 5,
              background: index === idx ? C.acc + "18" : "transparent",
              border: `0.5px solid ${index === idx ? C.acc + "66" : C.bdr}`,
              color: index === idx ? C.acc : C.dim,
              cursor: "pointer", fontWeight: index === idx ? 600 : 400,
            }}>{idx}</button>
          ))}
        </div>

        {/* ─── PÉRIODE ─────────────────────────────────────── */}
        <SepTitle>Période</SepTitle>

        {/* Années */}
        <div style={{ display: "flex", gap: 8, alignItems: "flex-end" }}>
          <div style={{ flex: 1 }}>
            <div style={lbl}>Année début</div>
            <input type="number"
              min={dsObj?.year_min || 1984} max={yearEnd} value={yearStart}
              onChange={e => { const v = parseInt(e.target.value); if (!isNaN(v)) setYearStart(Math.max(dsObj?.year_min || 1984, Math.min(yearEnd, v))); }}
              style={inp} />
          </div>
          <span style={{ fontSize: 16, color: C.dim, paddingBottom: 6 }}>→</span>
          <div style={{ flex: 1 }}>
            <div style={lbl}>Année fin</div>
            <input type="number"
              min={yearStart} max={dsObj?.year_max || 2025} value={yearEnd}
              onChange={e => { const v = parseInt(e.target.value); if (!isNaN(v)) setYearEnd(Math.min(dsObj?.year_max || 2025, Math.max(yearStart, v))); }}
              style={inp} />
          </div>
        </div>
        <div style={{ fontSize: 9, color: C.dim }}>Durée : {yearEnd - yearStart + 1} ans</div>

        {/* Fréquence */}
        <div>
          <div style={lbl}>Fréquence des frames</div>
          <div style={{ display: "flex", gap: 3 }}>
            {FREQ_OPTIONS.map(f => (
              <button key={f.id} onClick={() => setFreq(f.id)} title={f.desc} style={{
                fontFamily: F, fontSize: 10, padding: "5px 0", borderRadius: 5, flex: 1,
                background: freq === f.id ? C.acc + "18" : "transparent",
                border: `0.5px solid ${freq === f.id ? C.acc + "55" : C.bdr}`,
                color: freq === f.id ? C.acc : C.dim,
                cursor: "pointer", fontWeight: freq === f.id ? 600 : 400,
              }}>{f.label}</button>
            ))}
          </div>
        </div>

        {/* Mois */}
        <div>
          <div style={lbl}>Mois couverts par frame</div>
          <MonthRange startMonth={monthStart} endMonth={monthEnd}
            onChange={(s, e) => { setMonthStart(s); setMonthEnd(e); }} />
        </div>

        {/* ─── COMPOSITE ───────────────────────────────────── */}
        <SepTitle>Composite par frame</SepTitle>
        <div style={{ display: "flex", gap: 4 }}>
          {COMPOSITE_OPTIONS.map(c => (
            <button key={c.id} onClick={() => setComposite(c.id)} style={{
              fontFamily: F, fontSize: 10, padding: "5px 0", borderRadius: 5, flex: 1,
              background: composite === c.id ? C.acc + "18" : "transparent",
              border: `0.5px solid ${composite === c.id ? C.acc + "55" : C.bdr}`,
              color: composite === c.id ? C.acc : C.dim,
              cursor: "pointer", fontWeight: composite === c.id ? 600 : 400,
            }}>{c.label}</button>
          ))}
        </div>

        {/* Filtre nuages */}
        {dsObj?.has_cloud && (
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ fontSize: 9, color: C.dim, flexShrink: 0, display: "inline-flex", alignItems: "center", gap: 4 }}><IcCloud size={11}/> Max nuages</span>
            <input type="range" min={0} max={100} step={5} value={cloudMax}
              onChange={e => setCloudMax(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
            <span style={{ fontFamily: M, fontSize: 10, color: C.txt, flexShrink: 0 }}>{cloudMax}%</span>
          </div>
        )}

        {/* ─── ZONE D'ANALYSE ──────────────────────────────── */}
        <SepTitle>Zone d'analyse</SepTitle>
        <div style={{ display: "flex", gap: 4 }}>
          {[["bbox",IcMap,"Vue carte"],["layer",IcHexagon,"Couche (mask)"]].map(([m, Icon, label]) => (
            <button key={m} onClick={() => setRoiMode(m)} style={{
              fontFamily: F, flex: 1, fontSize: 10, padding: "5px 0", borderRadius: 5, cursor: "pointer",
              background: roiMode === m ? C.acc + "18" : "transparent",
              border: `0.5px solid ${roiMode === m ? C.acc + "55" : C.bdr}`,
              color: roiMode === m ? C.acc : C.dim,
              display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 5,
            }}><Icon size={12}/> {label}</button>
          ))}
        </div>
        {roiMode === "bbox" && (
          <div style={{ fontSize: 9, color: C.dim }}>Zoomez sur la zone avant de générer.</div>
        )}
        {roiMode === "layer" && (
          polyLayers.length === 0
            ? <div style={{ fontSize: 9, color: C.amb, padding: "5px 8px", background: C.amb + "12", borderRadius: 5 }}>
                Aucune couche polygone disponible.
              </div>
            : <select value={roiLayerId} onChange={e => setRoiLayerId(e.target.value)}
                style={{ fontFamily: F, fontSize: 10, padding: "5px 8px", borderRadius: 5, background: C.input, color: C.txt, border: `0.5px solid ${roiLayerId ? C.acc + "66" : C.bdr}`, outline: "none", width: "100%" }}>
                <option value="">— Choisir une couche —</option>
                {polyLayers.map(l => <option key={l.id} value={l.id}>{l.name} ({l.featureCount})</option>)}
              </select>
        )}

        {/* ─── PARAMÈTRES GIF ──────────────────────────────── */}
        <SepTitle>Paramètres GIF</SepTitle>

        {/* FPS */}
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 9, color: C.dim, flexShrink: 0, minWidth: 30 }}>FPS</span>
          <input type="range" min={1} max={10} step={1} value={fps}
            onChange={e => setFps(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
          <span style={{ fontFamily: M, fontSize: 10, color: C.txt, flexShrink: 0 }}>{fps}</span>
        </div>

        {/* Largeur */}
        <div>
          <div style={lbl}>Largeur</div>
          <div style={{ display: "flex", gap: 4 }}>
            {[256, 512, 768, 1024].map(w => (
              <button key={w} onClick={() => setGifWidth(w)} style={{
                fontFamily: M, fontSize: 9, padding: "4px 0", borderRadius: 4, flex: 1,
                background: gifWidth === w ? C.acc + "25" : "transparent",
                border: `0.5px solid ${gifWidth === w ? C.acc + "55" : C.bdr}`,
                color: gifWidth === w ? C.acc : C.dim, cursor: "pointer",
              }}>{w}px</button>
            ))}
          </div>
        </div>

        {/* Estimation */}
        <div style={{ fontSize: 9, color: C.dim, padding: "4px 8px", background: C.hover, borderRadius: 5 }}>
          ≈ {est} frames estimées · durée GIF ~{(est / fps).toFixed(0)}s à {fps} FPS
        </div>

        {/* ─── ANNOTATIONS ─────────────────────────────────── */}
        <SepTitle>Annotations</SepTitle>

        <div>
          <div style={lbl}>Titre</div>
          <input value={annTitle} onChange={e => setAnnTitle(e.target.value)}
            placeholder="ex : Évolution urbaine de Nantes" style={inp} />
        </div>

        <div>
          <div style={lbl}>Description</div>
          <input value={annDesc} onChange={e => setAnnDesc(e.target.value)}
            placeholder="ex : NDVI 2000–2024, bassin versant Loire" style={inp} />
        </div>

        <div>
          <div style={lbl}>Crédits</div>
          {/* "OpenMapAgents · GEE" est fixe et non modifiable */}
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <div style={{
              fontFamily: M, fontSize: 10, padding: "5px 7px", borderRadius: "5px 0 0 5px",
              background: C.hover, color: C.dim,
              border: `0.5px solid ${C.bdr}`, borderRight: "none",
              flexShrink: 0, whiteSpace: "nowrap",
            }}>OpenMapAgents · GEE</div>
            <input value={annCredits} onChange={e => setAnnCredits(e.target.value)}
              placeholder="· votre texte (optionnel)"
              style={{ ...inp, borderRadius: "0 5px 5px 0", flex: 1 }} />
          </div>
          <div style={{ fontSize: 9, color: C.dim, marginTop: 3 }}>
            La source satellite (ex : Landsat 8) est ajoutée automatiquement.
          </div>
        </div>

        <Toggle val={showNorth}    set={setShowNorth}    label="Flèche Nord (coin bas-droit)" />
        <Toggle val={showScale}    set={setShowScale}    label="Barre d'échelle (coin bas-droit)" />
        <Toggle val={showProgress} set={setShowProgress} label="Barre de progression temporelle" />
        <Toggle val={showLegend}   set={setShowLegend}   label="Légende palette (adaptée à la fréquence)" />

        {showLegend && (
          <div style={{ fontSize: 9, color: C.dim, padding: "4px 8px", background: C.hover, borderRadius: 5, lineHeight: 1.6 }}>
            annuel → 2000, 2001… · mensuel → Janv 2023…<br />
            saisonnier → Été 2023… · bimensuel → Jan-A 2023…
          </div>
        )}

        {/* ─── ERREUR ──────────────────────────────────────── */}
        {error && (
          <div style={{
            fontSize: 10, padding: "6px 8px", borderRadius: 5,
            background: C.red + "15", border: `0.5px solid ${C.red}44`,
            color: C.red, lineHeight: 1.5, display: "flex", gap: 5,
          }}><IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }}/> <span>{error}</span></div>
        )}

        {/* ─── PROGRESSION ─────────────────────────────────── */}
        {progress && !error && (
          <div style={{
            fontSize: 10, padding: "6px 8px", borderRadius: 5,
            background: C.amb + "15", border: `0.5px solid ${C.amb}44`,
            color: C.amb, lineHeight: 1.5, display: "flex", alignItems: "center", gap: 6,
          }}>
            <span style={{
              display: "inline-block", width: 10, height: 10, borderRadius: "50%",
              border: `2px solid ${C.amb}44`, borderTop: `2px solid ${C.amb}`,
              animation: "spin .8s linear infinite", flexShrink: 0,
            }} />
            {progress}
          </div>
        )}

        {/* ─── RÉSULTAT ────────────────────────────────────── */}
        {gifResult?.url && !loading && (
          <GifPreview url={gifResult.url} status={gifResult.status} onDownload={download} />
        )}

        {/* ─── BOUTON GÉNÉRER ──────────────────────────────── */}
        <button onClick={generate} disabled={loading} style={{
          fontFamily: F, fontSize: 12, fontWeight: 600,
          padding: "10px 0", borderRadius: 7, width: "100%",
          background: loading ? C.hover : C.acc,
          color: loading ? C.dim : "#fff",
          border: "none", cursor: loading ? "default" : "pointer",
          opacity: loading ? 0.7 : 1, marginTop: 4,
          display: "flex", alignItems: "center", justifyContent: "center", gap: 7,
        }}>
          <IcFilm size={14}/> {loading ? "Génération en cours…" : "Générer le Timelapse GIF"}
        </button>
        <div style={{ fontSize: 9, color: C.dim, textAlign: "center" }}>
          30 – 120 sec selon la zone et la période
        </div>

        {/* Espace bas pour ne pas coller au bouton */}
        <div style={{ height: 8 }} />

      </div>
    </div>
  );
}
