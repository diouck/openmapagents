/**
 * ComparatorPanel.jsx — Comparateur A/B raster & vecteur
 *
 * Technique : deux instances MapLibre superposées.
 * La carte B est clippée via clip-path CSS dynamique selon la position du slider.
 * Fonctionne pour les couches raster (GEE tiles) ET vecteur (GeoJSON).
 *
 * Props :
 *   layers      — toutes les couches disponibles
 *   vs          — viewState de la carte principale { longitude, latitude, zoom, pitch, bearing }
 *   mapStyle    — URL du style actuel (OpenFreeMap)
 *   mapRef      — ref de la carte principale (pour sync centre/zoom)
 *   getPaint    — fonction getPaint(layer, geomType) de App.jsx
 */

import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import Map, { Source, Layer } from "react-map-gl/maplibre";
import { useThemeContext } from "../theme";
import { F, M, MAP_STYLES } from "../config";
import { IcMap, IcHexagon, IcCheck } from "../icons";

// ── Séparateur ────────────────────────────────────────────────
function Sep({ children }) {
  const C = useThemeContext();
  return (
    <div style={{
      fontSize: 9, color: C.dim, textTransform: "uppercase",
      letterSpacing: ".06em", fontWeight: 600,
      borderBottom: `0.5px solid ${C.bdr}`, paddingBottom: 4, marginTop: 2,
    }}>{children}</div>
  );
}

// ── Sélecteur de couche ───────────────────────────────────────
function LayerSelect({ value, onChange, layers, label, accent }) {
  const C = useThemeContext();
  return (
    <div>
      <div style={{ fontSize: 9, color: C.dim, marginBottom: 3 }}>{label}</div>
      <select value={value} onChange={e => onChange(e.target.value)}
        style={{
          fontFamily: F, fontSize: 10, padding: "5px 8px", borderRadius: 5, width: "100%",
          background: C.input, color: C.txt,
          border: `0.5px solid ${value ? accent + "88" : C.bdr}`,
          outline: "none",
        }}>
        <option value="">— Choisir une couche —</option>
        {layers.map(l => (
          <option key={l.id} value={l.id}>
            {l.isRaster ? "Raster · " : "Vecteur · "}{l.name}
          </option>
        ))}
      </select>
    </div>
  );
}

// ── Rendu des couches sur une instance Map ─────────────────────
function LayersRenderer({ layer }) {
  if (!layer) return null;

  if (layer.isRaster) {
    return (
      <Source id={`comp-${layer.id}`} type="raster" tiles={[layer.tileUrl]} tileSize={256}>
        <Layer id={`comp-${layer.id}-layer`} type="raster"
          paint={{ "raster-opacity": layer.opacity ?? 0.9 }} />
      </Source>
    );
  }

  // Couche vectorielle
  const feats = layer.geojson?.features || [];
  if (!feats.length) return null;
  const cr = layer.classResult;
  const ce = cr?.expression || layer.color;

  return (
    <Source id={`comp-${layer.id}`} type="geojson" data={layer.geojson}>
      <Layer id={`comp-${layer.id}-fill`} type="fill"
        filter={["any", ["==", ["geometry-type"], "Polygon"], ["==", ["geometry-type"], "MultiPolygon"]]}
        paint={{ "fill-color": ce, "fill-opacity": (layer.opacity ?? 0.8) * 0.4 }} />
      <Layer id={`comp-${layer.id}-line`} type="line"
        filter={["any", ["==", ["geometry-type"], "LineString"], ["==", ["geometry-type"], "Polygon"], ["==", ["geometry-type"], "MultiPolygon"]]}
        paint={{ "line-color": ce, "line-width": 1.5, "line-opacity": layer.opacity ?? 0.9 }} />
      <Layer id={`comp-${layer.id}-circle`} type="circle"
        filter={["==", ["geometry-type"], "Point"]}
        paint={{ "circle-radius": layer.radius || 5, "circle-color": ce, "circle-opacity": layer.opacity ?? 0.9, "circle-stroke-width": 1, "circle-stroke-color": "#fff" }} />
    </Source>
  );
}

// ════════════════════════════════════════════════════════════════
export default function ComparatorPanel({ layers = [], vs, mapStyle }) {
  const C = useThemeContext();

  const [layerIdA, setLayerIdA] = useState("");
  const [layerIdB, setLayerIdB] = useState("");
  const [active,   setActive]   = useState(false);   // comparateur ouvert ou non
  const [sliderX,  setSliderX]  = useState(50);       // % 0-100
  const [styleA,   setStyleA]   = useState("positron");
  const [styleB,   setStyleB]   = useState("positron");
  const [labelsOn, setLabelsOn] = useState(true);

  // ViewState partagé entre les deux cartes (sync avec la carte principale)
  const [sharedVs, setSharedVs] = useState(vs);
  useEffect(() => { setSharedVs(vs); }, [vs]);

  const mapARef   = useRef(null);
  const mapBRef   = useRef(null);
  const sliderRef = useRef(null);
  const dragging  = useRef(false);
  const containerRef = useRef(null);

  const layerA = layers.find(l => l.id === layerIdA) || null;
  const layerB = layers.find(l => l.id === layerIdB) || null;

  // ── Drag slider ─────────────────────────────────────────────
  const onSliderMouseDown = useCallback((e) => {
    e.preventDefault();
    dragging.current = true;
    const move = (ev) => {
      if (!dragging.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const x = Math.max(5, Math.min(95, ((ev.clientX - rect.left) / rect.width) * 100));
      setSliderX(x);
    };
    const up = () => { dragging.current = false; window.removeEventListener("mousemove", move); window.removeEventListener("mouseup", up); };
    window.addEventListener("mousemove", move);
    window.addEventListener("mouseup", up);
  }, []);

  // Touch support
  const onSliderTouchStart = useCallback((e) => {
    const touch = e.touches[0];
    const move = (ev) => {
      if (!containerRef.current) return;
      const t = ev.touches[0];
      const rect = containerRef.current.getBoundingClientRect();
      const x = Math.max(5, Math.min(95, ((t.clientX - rect.left) / rect.width) * 100));
      setSliderX(x);
    };
    const end = () => { window.removeEventListener("touchmove", move); window.removeEventListener("touchend", end); };
    window.addEventListener("touchmove", move);
    window.addEventListener("touchend", end);
  }, []);

  // ── Sync viewstate entre les deux cartes ────────────────────
  const onMoveA = useCallback((e) => {
    setSharedVs(e.viewState);
    if (mapBRef.current) {
      try { mapBRef.current.getMap?.()?.jumpTo({ center:[e.viewState.longitude, e.viewState.latitude], zoom:e.viewState.zoom, bearing:e.viewState.bearing, pitch:e.viewState.pitch }); } catch(_) {}
    }
  }, []);

  const onMoveB = useCallback((e) => {
    setSharedVs(e.viewState);
    if (mapARef.current) {
      try { mapARef.current.getMap?.()?.jumpTo({ center:[e.viewState.longitude, e.viewState.latitude], zoom:e.viewState.zoom, bearing:e.viewState.bearing, pitch:e.viewState.pitch }); } catch(_) {}
    }
  }, []);

  const curStyle  = mapStyle || MAP_STYLES.positron;
  const styleUrlA = MAP_STYLES[styleA] || curStyle;
  const styleUrlB = MAP_STYLES[styleB] || curStyle;

  const mapCommonProps = {
    style: { width: "100%", height: "100%" },
    maplibreLogo: false,
    attributionControl: false,
    ...sharedVs,
  };

  // Clip-path pour la carte B : révèle la partie droite du slider
  const clipB = `polygon(${sliderX}% 0%, 100% 0%, 100% 100%, ${sliderX}% 100%)`;

  // ── Panel fermé : sélecteur de couches ─────────────────────
  if (!active) {
    return (
      <div style={{ display:"flex", flexDirection:"column", width:"100%", height:"100%", minHeight:0, overflow:"hidden" }}>
        <div style={{ padding:"5px 12px", borderBottom:`0.5px solid ${C.bdr}`, fontSize:10, color:C.dim, flexShrink:0, display:"flex", alignItems:"center", gap:6 }}>
          <span>◧</span><span>Comparateur A/B · Raster & Vecteur</span>
        </div>
        <div style={{ flex:1, minHeight:0, overflowY:"auto", overflowX:"hidden", padding:"12px 14px", display:"flex", flexDirection:"column", gap:10 }}>

          <Sep>Couche A (gauche)</Sep>
          <LayerSelect value={layerIdA} onChange={setLayerIdA} layers={layers} label="Couche A" accent="#378ADD" />

          {/* Style de fond pour A */}
          <div>
            <div style={{ fontSize:9, color:C.dim, marginBottom:3 }}>Fond de carte A</div>
            <div style={{ display:"flex", gap:3 }}>
              {Object.keys(MAP_STYLES).map(k => (
                <button key={k} onClick={() => setStyleA(k)} style={{
                  fontFamily:F, fontSize:9, flex:1, padding:"4px 0", borderRadius:4,
                  background: styleA===k ? "#378ADD18" : "transparent",
                  border: `0.5px solid ${styleA===k ? "#378ADD55" : C.bdr}`,
                  color: styleA===k ? "#378ADD" : C.dim, cursor:"pointer",
                }}>{k}</button>
              ))}
            </div>
          </div>

          <Sep>Couche B (droite)</Sep>
          <LayerSelect value={layerIdB} onChange={setLayerIdB} layers={layers} label="Couche B" accent="#E24B4A" />

          {/* Style de fond pour B */}
          <div>
            <div style={{ fontSize:9, color:C.dim, marginBottom:3 }}>Fond de carte B</div>
            <div style={{ display:"flex", gap:3 }}>
              {Object.keys(MAP_STYLES).map(k => (
                <button key={k} onClick={() => setStyleB(k)} style={{
                  fontFamily:F, fontSize:9, flex:1, padding:"4px 0", borderRadius:4,
                  background: styleB===k ? "#E24B4A18" : "transparent",
                  border: `0.5px solid ${styleB===k ? "#E24B4A55" : C.bdr}`,
                  color: styleB===k ? "#E24B4A" : C.dim, cursor:"pointer",
                }}>{k}</button>
              ))}
            </div>
          </div>

          <Sep>Options</Sep>
          <div style={{ display:"flex", alignItems:"center", gap:8 }}>
            <button onClick={() => setLabelsOn(v => !v)} style={{
              fontFamily:F, fontSize:9, padding:"3px 8px", borderRadius:4, flexShrink:0,
              background: labelsOn ? C.acc+"18" : "transparent",
              border: `0.5px solid ${labelsOn ? C.acc+"55" : C.bdr}`,
              color: labelsOn ? C.acc : C.dim, cursor:"pointer", display:"inline-flex", alignItems:"center", gap:4,
            }}>{labelsOn ? <><IcCheck size={11}/> On</> : "Off"}</button>
            <span style={{ fontSize:10, color:C.mut }}>Labels A / B sur le slider</span>
          </div>

          {/* Info types */}
          {layerA && (
            <div style={{ fontSize:9, color:C.dim, padding:"4px 8px", background:C.hover, borderRadius:5, display:"flex", alignItems:"center", gap:5 }}>
              A : {layerA.isRaster ? <IcMap size={11}/> : <IcHexagon size={11}/>} {layerA.isRaster ? "Raster (tuiles WMS)" : `Vecteur (${layerA.featureCount} entités)`}
            </div>
          )}
          {layerB && (
            <div style={{ fontSize:9, color:C.dim, padding:"4px 8px", background:C.hover, borderRadius:5, display:"flex", alignItems:"center", gap:5 }}>
              B : {layerB.isRaster ? <IcMap size={11}/> : <IcHexagon size={11}/>} {layerB.isRaster ? "Raster (tuiles WMS)" : `Vecteur (${layerB.featureCount} entités)`}
            </div>
          )}

          {/* Bouton lancer */}
          <div style={{ marginTop:"auto" }}>
            <button
              onClick={() => setActive(true)}
              disabled={!layerIdA || !layerIdB}
              style={{
                fontFamily:F, fontSize:12, fontWeight:600,
                padding:"10px 0", borderRadius:7, width:"100%",
                background: (layerIdA && layerIdB) ? C.acc : C.hover,
                color:      (layerIdA && layerIdB) ? "#fff" : C.dim,
                border:"none", cursor:(layerIdA && layerIdB) ? "pointer" : "default",
              }}>
              ◧ Ouvrir le comparateur
            </button>
            {(!layerIdA || !layerIdB) && (
              <div style={{ fontSize:9, color:C.dim, textAlign:"center", marginTop:4 }}>
                Sélectionnez les couches A et B pour continuer
              </div>
            )}
          </div>
          <div style={{ height:8 }}/>
        </div>
      </div>
    );
  }

  // ── Comparateur actif : plein écran dans le FloatingPanel ────
  return (
    <div style={{ display:"flex", flexDirection:"column", width:"100%", height:"100%", minHeight:0, overflow:"hidden" }}>

      {/* Barre de contrôle */}
      <div style={{
        display:"flex", alignItems:"center", gap:8,
        padding:"5px 10px", borderBottom:`0.5px solid ${C.bdr}`,
        flexShrink:0, background:C.card,
      }}>
        <button onClick={() => setActive(false)} style={{
          fontFamily:F, fontSize:10, padding:"3px 8px", borderRadius:5,
          background:"transparent", border:`0.5px solid ${C.bdr}`,
          color:C.mut, cursor:"pointer",
        }}>← Modifier</button>

        <div style={{ display:"flex", alignItems:"center", gap:5, flex:1, justifyContent:"center" }}>
          <div style={{ width:8, height:8, borderRadius:"50%", background:"#378ADD" }}/>
          <span style={{ fontSize:10, color:C.txt, maxWidth:100, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{layerA?.name}</span>
          <span style={{ fontSize:10, color:C.dim }}>vs</span>
          <span style={{ fontSize:10, color:C.txt, maxWidth:100, overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{layerB?.name}</span>
          <div style={{ width:8, height:8, borderRadius:"50%", background:"#E24B4A" }}/>
        </div>

        {/* Slider position numérique */}
        <span style={{ fontFamily:M, fontSize:9, color:C.dim, flexShrink:0 }}>
          {Math.round(sliderX)}%
        </span>
      </div>

      {/* Zone des deux cartes superposées */}
      <div
        ref={containerRef}
        style={{ flex:1, position:"relative", overflow:"hidden", cursor:"ew-resize" }}
      >
        {/* Carte A — dessous, pleine largeur */}
        <div style={{ position:"absolute", inset:0 }}>
          <Map
            ref={mapARef}
            {...mapCommonProps}
            mapStyle={styleUrlA}
            onMove={onMoveA}
          >
            <LayersRenderer layer={layerA} />
          </Map>
        </div>

        {/* Carte B — dessus, clippée à droite du slider */}
        <div style={{ position:"absolute", inset:0, clipPath:clipB }}>
          <Map
            ref={mapBRef}
            {...mapCommonProps}
            mapStyle={styleUrlB}
            onMove={onMoveB}
          >
            <LayersRenderer layer={layerB} />
          </Map>
        </div>

        {/* Ligne du slider */}
        <div
          ref={sliderRef}
          onMouseDown={onSliderMouseDown}
          onTouchStart={onSliderTouchStart}
          style={{
            position:"absolute", top:0, bottom:0,
            left:`${sliderX}%`,
            width:3,
            background:"#fff",
            boxShadow:"0 0 6px rgba(0,0,0,0.4)",
            cursor:"ew-resize",
            zIndex:10,
            transform:"translateX(-50%)",
          }}
        >
          {/* Poignée centrale */}
          <div style={{
            position:"absolute", top:"50%", left:"50%",
            transform:"translate(-50%, -50%)",
            width:32, height:32, borderRadius:"50%",
            background:"#fff",
            boxShadow:"0 2px 8px rgba(0,0,0,0.3)",
            display:"flex", alignItems:"center", justifyContent:"center",
            fontSize:14, userSelect:"none",
          }}>⇔</div>
        </div>

        {/* Labels A / B */}
        {labelsOn && (<>
          <div style={{
            position:"absolute", top:10, left:10, zIndex:11,
            background:"#378ADD", color:"#fff",
            fontSize:11, fontWeight:700, fontFamily:F,
            padding:"3px 10px", borderRadius:5,
            boxShadow:"0 1px 4px rgba(0,0,0,0.2)",
            maxWidth:`${sliderX - 4}%`,
            overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap",
          }}>
            A · {layerA?.name}
          </div>
          <div style={{
            position:"absolute", top:10, right:10, zIndex:11,
            background:"#E24B4A", color:"#fff",
            fontSize:11, fontWeight:700, fontFamily:F,
            padding:"3px 10px", borderRadius:5,
            boxShadow:"0 1px 4px rgba(0,0,0,0.2)",
            maxWidth:`${100 - sliderX - 4}%`,
            overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap",
          }}>
            B · {layerB?.name}
          </div>
        </>)}
      </div>
    </div>
  );
}
