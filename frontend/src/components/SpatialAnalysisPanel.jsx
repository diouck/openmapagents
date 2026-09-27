/**
 * SpatialAnalysisPanel.jsx — Grand panneau d'analyse spatiale unifié.
 * =============================================================================
 * Filtre en haut : Vecteur | Raster | Avancé. Chaque mode garde son sous-panneau
 * intact (SpatialPanel pour le vecteur, WhiteboxPanel pour le raster/avancé) —
 * on ne réécrit pas la logique existante, on l'unifie sous une seule entrée de
 * menu « Analyse spatiale ».
 * =============================================================================
 */
import { useState } from "react";
import { useThemeContext } from "../theme";
import { F } from "../config";
import SpatialPanel from "./SpatialPanel";
import WhiteboxPanel from "./WhiteboxPanel";
import { IcVenn, IcMountain, IcSparkles } from "../icons";

const MODES = [
  { id: "vecteur", label: "Vecteur", Icon: IcVenn },
  { id: "raster", label: "Raster", Icon: IcMountain },
  { id: "avance", label: "Avancé", Icon: IcSparkles },
];

// Catégories raster « standard » (tout sauf la catégorie Avancé)
const RASTER_CATS = ["morphologie", "hydrologie", "filtres", "nettoyage", "segmentation", "stats_locales", "image", "distance", "reclassification"];

export default function SpatialAnalysisPanel({ layers = [], onAddLayer, onAddRasterLayer, mapRef }) {
  const C = useThemeContext();
  const [mode, setMode] = useState("vecteur");

  const vectorLayers = layers.filter(l => l.visible && !l.isRaster);

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", minHeight: 0 }}>
      {/* ── Filtre Vecteur / Raster / Avancé ── */}
      <div style={{ display: "flex", gap: 4, padding: "10px 12px", borderBottom: `0.5px solid ${C.bdr}`, flexShrink: 0 }}>
        {MODES.map(({ id, label, Icon }) => (
          <button
            key={id}
            onClick={() => setMode(id)}
            style={{
              flex: 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
              padding: "8px 10px", borderRadius: 7,
              border: `0.5px solid ${mode === id ? C.acc : C.bdr}`,
              background: mode === id ? C.acc : "transparent",
              color: mode === id ? "#fff" : C.mut,
              fontFamily: F, fontSize: 12.5, fontWeight: 600, cursor: "pointer", transition: "all 0.15s",
            }}
          >
            <Icon size={15} />{label}
          </button>
        ))}
      </div>

      {/* ── Sous-panneau selon le mode ── */}
      <div style={{ flex: 1, minHeight: 0, overflow: "hidden" }}>
        {mode === "vecteur" && (
          <SpatialPanel layers={vectorLayers} onAddLayer={onAddLayer} />
        )}
        {mode === "raster" && (
          <WhiteboxPanel onAddRasterLayer={onAddRasterLayer} mapRef={mapRef} layers={layers} filterCategories={RASTER_CATS} />
        )}
        {mode === "avance" && (
          <WhiteboxPanel onAddRasterLayer={onAddRasterLayer} mapRef={mapRef} layers={layers} filterCategories={["avance"]} />
        )}
      </div>
    </div>
  );
}
