/**
 * GraticulePanel.jsx — Plugin « Graticule » : grille lat/lon superposée.
 * =============================================================================
 * 100 % client : construit un GeoJSON de méridiens + parallèles et l'ajoute à la
 * carte (source + couche ligne + labels optionnels). Fonctionne MapLibre & Mapbox
 * (même API addSource/addLayer). Nettoie ses couches au démontage.
 *
 * Le composant NE dessine PAS son propre cadre ni son titre : il vit à l'intérieur
 * du FloatingPanel (en-tête « Graticule (grille lat/lon) » + bord + redimension).
 * Il remplit la largeur et laisse sa hauteur naturelle piloter le panneau « auto ».
 * =============================================================================
 */
import { useState, useEffect } from "react";
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

// Pas conseillé selon le zoom (mode « Auto »). Borné à 2° pour rester léger.
const stepFromZoom = (z) => {
  if (z == null) return 10;
  if (z < 2)   return 30;
  if (z < 3.5) return 15;
  if (z < 5)   return 10;
  if (z < 7)   return 5;
  return 2;
};

export default function GraticulePanel({ mapRef }) {
  const C = useThemeContext();
  const [tab, setTab]         = useState("settings");   // settings | about
  const [step, setStep]       = useState(10);           // number | "auto"
  const [zoomStep, setZoomStep] = useState(10);         // pas effectif quand step === "auto"
  const [labels, setLabels]   = useState(true);
  const [color, setColor]     = useState("#6aa0ff");
  const [opacity, setOpacity] = useState(0.7);

  const eff = step === "auto" ? zoomStep : step;        // pas réellement dessiné

  // Mode auto : le pas suit le zoom de la carte (recalcul à chaque déplacement).
  useEffect(() => {
    const map = mapRef.current?.getMap?.();
    if (!map || step !== "auto") return;
    const update = () => setZoomStep(stepFromZoom(map.getZoom()));
    update();
    map.on("moveend", update);
    return () => { try { map.off("moveend", update); } catch (_) {} };
  }, [step, mapRef]);

  // (Re)construit / met à jour la grille à chaque changement de paramètre.
  useEffect(() => {
    const map = mapRef.current?.getMap?.();
    if (!map) return;

    const apply = () => {
      const data = buildGraticule(eff);
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
  }, [eff, labels, color, opacity, mapRef]);

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

  const lbl = { fontSize: 11, color: C.mut, marginBottom: 6 };

  const tabBtn = (id, l) => (
    <button key={id} onClick={() => setTab(id)} style={{
      flex: 1, fontFamily: F, fontSize: 12, padding: "7px 0", borderRadius: 8, cursor: "pointer",
      border: `0.5px solid ${tab === id ? C.acc : C.bdr}`,
      background: tab === id ? C.acc + "18" : "transparent",
      color: tab === id ? C.acc : C.mut, fontWeight: tab === id ? 600 : 400,
    }}>{l}</button>
  );

  const stepBtn = (v, l) => {
    const on = step === v;
    return (
      <button key={String(v)} onClick={() => setStep(v)} style={{
        flex: 1, minWidth: 40, fontFamily: F, fontSize: 12, padding: "6px 0", borderRadius: 7, cursor: "pointer",
        border: `0.5px solid ${on ? C.acc : C.bdr}`,
        background: on ? C.acc : "transparent",
        color: on ? "#04120a" : C.mut, fontWeight: on ? 600 : 400,
      }}>{l}</button>
    );
  };

  return (
    <div style={{ fontFamily: F, width: "100%", boxSizing: "border-box", padding: "10px 12px 13px", display: "flex", flexDirection: "column", gap: 12 }}>

      {/* Onglets */}
      <div style={{ display: "flex", gap: 6 }}>
        {tabBtn("settings", "Réglages")}
        {tabBtn("about", "Définition")}
      </div>

      {tab === "settings" ? (
        <>
          {/* Espacement (Auto = selon le zoom) */}
          <div>
            <div style={lbl}>Espacement</div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              {stepBtn("auto", "Auto")}{stepBtn(5, "5°")}{stepBtn(10, "10°")}{stepBtn(15, "15°")}{stepBtn(30, "30°")}
            </div>
            {step === "auto" && (
              <div style={{ fontSize: 10.5, color: C.dim, marginTop: 6 }}>
                Pas adapté au niveau de zoom — actuellement {eff}°.
              </div>
            )}
          </div>

          {/* Opacité */}
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontSize: 11, color: C.mut, width: 58 }}>Opacité</span>
            <input type="range" min={0.1} max={1} step={0.1} value={opacity} onChange={(e) => setOpacity(+e.target.value)} style={{ flex: 1, minWidth: 0 }} />
            <span style={{ fontSize: 11, color: C.txt, width: 30, textAlign: "right" }}>{Math.round(opacity * 100)}%</span>
          </div>

          {/* Couleur */}
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontSize: 11, color: C.mut, width: 58 }}>Couleur</span>
            <input type="color" value={color} onChange={(e) => setColor(e.target.value)} style={{ width: 34, height: 24, background: "transparent", border: `0.5px solid ${C.bdr}`, borderRadius: 6, cursor: "pointer" }} />
          </div>

          {/* Degrés */}
          <label style={{ display: "flex", alignItems: "center", gap: 8, cursor: "pointer" }}>
            <input type="checkbox" checked={labels} onChange={(e) => setLabels(e.target.checked)} />
            <span style={{ fontSize: 12, color: C.txt }}>Afficher les degrés</span>
          </label>
        </>
      ) : (
        <div style={{ fontSize: 11.5, color: C.mut, lineHeight: 1.55, display: "flex", flexDirection: "column", gap: 8 }}>
          <p style={{ margin: 0 }}>
            Le <b style={{ color: C.txt }}>graticule</b> est le quadrillage des <b style={{ color: C.txt }}>méridiens</b> (lignes de longitude, orientées nord-sud) et des <b style={{ color: C.txt }}>parallèles</b> (lignes de latitude, est-ouest), exprimés en degrés.
          </p>
          <p style={{ margin: 0 }}>
            Il sert de repère de coordonnées géographiques (WGS84) : lire une position, estimer un écart angulaire, ou caler une capture / une impression.
          </p>
          <p style={{ margin: 0 }}>
            Origines : l'équateur = 0° de latitude, le méridien de Greenwich = 0° de longitude. Latitude de −90° à +90°, longitude de −180° à +180°.
          </p>
        </div>
      )}
    </div>
  );
}
