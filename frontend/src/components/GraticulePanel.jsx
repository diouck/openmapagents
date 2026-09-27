/**
 * GraticulePanel.jsx — Plugin « Graticule » : grille lat/lon superposée.
 * =============================================================================
 * 100 % client : construit un GeoJSON de méridiens + parallèles et l'ajoute à la
 * carte (source + couche ligne + labels optionnels). Fonctionne MapLibre & Mapbox
 * (même API addSource/addLayer). Nettoie ses couches au démontage.
 * =============================================================================
 */
import { useState, useEffect, useRef } from "react";
import { useThemeContext } from "../theme";
import { F } from "../config";

const SRC = "oma-graticule";
const LINE = "oma-graticule-lines";
const LAB = "oma-graticule-labels";

function buildGraticule(step) {
  const feats = [];
  for (let lon = -180; lon <= 180; lon += step) {
    const coords = [];
    for (let lat = -80; lat <= 80; lat += 2) coords.push([lon, lat]);
    feats.push({ type: "Feature", geometry: { type: "LineString", coordinates: coords }, properties: { value: lon } });
  }
  for (let lat = -80; lat <= 80; lat += step) {
    const coords = [];
    for (let lon = -180; lon <= 180; lon += 2) coords.push([lon, lat]);
    feats.push({ type: "Feature", geometry: { type: "LineString", coordinates: coords }, properties: { value: lat } });
  }
  return { type: "FeatureCollection", features: feats };
}

export default function GraticulePanel({ mapRef }) {
  const C = useThemeContext();
  const [step, setStep] = useState(10);
  const [labels, setLabels] = useState(true);
  const [color, setColor] = useState("#6aa0ff");
  const [opacity, setOpacity] = useState(0.7);

  // (Re)construit / met à jour la grille à chaque changement de paramètre.
  useEffect(() => {
    const map = mapRef.current?.getMap?.();
    if (!map) return;

    const apply = () => {
      const data = buildGraticule(step);
      try {
        if (map.getSource(SRC)) {
          map.getSource(SRC).setData(data);
        } else {
          map.addSource(SRC, { type: "geojson", data });
          map.addLayer({
            id: LINE, type: "line", source: SRC,
            paint: { "line-color": color, "line-width": 0.6, "line-opacity": opacity },
          });
          // Labels : couche symbole (police du style courant). En try/catch car
          // certains fonds n'exposent pas de glyphes — la grille reste visible sans.
          try {
            map.addLayer({
              id: LAB, type: "symbol", source: SRC,
              layout: {
                "symbol-placement": "line", "symbol-spacing": 200,
                "text-field": ["concat", ["to-string", ["get", "value"]], "°"],
                "text-size": 10, "text-keep-upright": true,
              },
              paint: { "text-color": color, "text-halo-color": "rgba(0,0,0,.65)", "text-halo-width": 1.2 },
            });
          } catch (_) { /* pas de glyphes sur ce fond : on garde juste les lignes */ }
        }
        if (map.getLayer(LINE)) {
          map.setPaintProperty(LINE, "line-color", color);
          map.setPaintProperty(LINE, "line-opacity", opacity);
        }
        if (map.getLayer(LAB)) {
          map.setPaintProperty(LAB, "text-color", color);
          map.setLayoutProperty(LAB, "visibility", labels ? "visible" : "none");
        }
      } catch (_) { /* style en cours de (re)chargement : réessayé au prochain styledata */ }
    };

    if (map.isStyleLoaded && map.isStyleLoaded()) apply();
    else map.once("styledata", apply);
  }, [step, labels, color, opacity, mapRef]);

  // Nettoyage au démontage (fermeture du panneau).
  useEffect(() => () => {
    const map = mapRef.current?.getMap?.();
    if (!map) return;
    try {
      if (map.getLayer(LAB)) map.removeLayer(LAB);
      if (map.getLayer(LINE)) map.removeLayer(LINE);
      if (map.getSource(SRC)) map.removeSource(SRC);
    } catch (_) { /* déjà retiré */ }
  }, [mapRef]);

  const stepBtn = (v, l) => (
    <button key={v} onClick={() => setStep(v)} style={{
      flex: 1, fontSize: 12, padding: "6px 0", borderRadius: 7, cursor: "pointer",
      border: `0.5px solid ${step === v ? C.acc : C.bdr}`,
      background: step === v ? C.acc : "transparent",
      color: step === v ? "#04120a" : C.mut, fontWeight: step === v ? 600 : 400,
    }}>{l}</button>
  );

  return (
    <div style={{ width: 250, background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 12, overflow: "hidden", fontFamily: F }}>
      <div style={{ padding: "11px 13px", borderBottom: `0.5px solid ${C.bdr}` }}>
        <div style={{ fontSize: 13, fontWeight: 600, color: C.txt }}>Graticule</div>
        <div style={{ fontSize: 10.5, color: C.dim, marginTop: 2 }}>Grille latitude / longitude sur la carte.</div>
      </div>

      <div style={{ padding: "12px 13px" }}>
        <div style={{ fontSize: 11, color: C.mut, marginBottom: 6 }}>Espacement</div>
        <div style={{ display: "flex", gap: 6 }}>{stepBtn(5, "5°")}{stepBtn(10, "10°")}{stepBtn(15, "15°")}{stepBtn(30, "30°")}</div>

        <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 13 }}>
          <span style={{ fontSize: 11, color: C.mut, width: 62 }}>Opacité</span>
          <input type="range" min={0.1} max={1} step={0.1} value={opacity} onChange={(e) => setOpacity(+e.target.value)} style={{ flex: 1 }} />
          <span style={{ fontSize: 11, color: C.txt, width: 28, textAlign: "right" }}>{Math.round(opacity * 100)}%</span>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 11 }}>
          <span style={{ fontSize: 11, color: C.mut, width: 62 }}>Couleur</span>
          <input type="color" value={color} onChange={(e) => setColor(e.target.value)} style={{ width: 34, height: 24, background: "transparent", border: `0.5px solid ${C.bdr}`, borderRadius: 6, cursor: "pointer" }} />
        </div>

        <label style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 13, cursor: "pointer" }}>
          <input type="checkbox" checked={labels} onChange={(e) => setLabels(e.target.checked)} />
          <span style={{ fontSize: 12, color: C.txt }}>Afficher les degrés</span>
        </label>
      </div>
    </div>
  );
}
