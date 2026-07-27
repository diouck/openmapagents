/**
 * TimelapseModal.jsx — Modale d'affichage du timelapse GEE généré par le LLM
 *
 * NE PAS CONFONDRE avec TimelapsePanel.jsx (panneau manuel).
 * Ce composant est uniquement déclenché par le chat agent quand
 * handleToolResult reçoit { type: "add_timelapse", gif_url, ... }
 *
 * Props :
 *   timelapse : { gif_url, frames, period, size_mb, dataset, index, source }
 *   onClose   : () => void
 */

import { useEffect, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, API } from "../config";
import { IcFilm, IcX, IcAlert, IcFileDown } from "../icons";

// Labels lisibles
const DATASET_LABELS = {
  sentinel2: "Sentinel-2 (10m)",
  landsat:   "Landsat (30m)",
  landsat8:  "Landsat 8 (30m)",
  landsat9:  "Landsat 9 (30m)",
};

const INDEX_LABELS = {
  RGB:  "Couleurs naturelles (RGB)",
  NDVI: "Végétation (NDVI)",
  NDWI: "Eau (NDWI)",
};

export default function TimelapseModal({ timelapse, onClose }) {
  const C      = useThemeContext();
  const imgRef = useRef(null);

  // Fermer avec Escape
  useEffect(() => {
    const handler = (e) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  if (!timelapse?.gif_url) return null;

  // gif_url = "/api/gee/timelapse/file/xxx.gif"
  // API     = "http://localhost:8000/api" → retire le /api du prefixe
  const baseUrl  = API.replace(/\/api$/, "");
  const gifSrc   = timelapse.gif_url.startsWith("http")
    ? timelapse.gif_url
    : `${baseUrl}${timelapse.gif_url}`;

  const dataset  = DATASET_LABELS[timelapse.dataset] || timelapse.dataset || "Satellite";
  const index    = INDEX_LABELS[timelapse.index]    || timelapse.index    || "RGB";
  const period   = timelapse.period  || "";
  const frames   = timelapse.frames  || "?";
  const sizeMb   = timelapse.size_mb ? `${timelapse.size_mb.toFixed(1)} MB` : "";
  const source   = timelapse.source  || dataset;

  const handleDownload = () => {
    const a = document.createElement("a");
    a.href     = gifSrc;
    a.download = `timelapse_${timelapse.dataset}_${timelapse.index}_${Date.now()}.gif`;
    a.click();
  };

  return (
    <>
      {/* ── Overlay ──────────────────────────────────────────── */}
      <div
        onClick={onClose}
        style={{
          position: "fixed", inset: 0,
          background: "rgba(0,0,0,0.65)",
          zIndex: 10000,
          backdropFilter: "blur(3px)",
        }}
      />

      {/* ── Fenêtre modale ───────────────────────────────────── */}
      <div style={{
        position:  "fixed",
        top:       "50%",
        left:      "50%",
        transform: "translate(-50%, -50%)",
        zIndex:    10001,
        background: C.card,
        border:     `1px solid ${C.bdr}`,
        borderRadius: 12,
        boxShadow:  "0 20px 60px rgba(0,0,0,0.5)",
        maxWidth:   600,
        width:      "90vw",
        maxHeight:  "90vh",
        display:    "flex",
        flexDirection: "column",
        overflow:   "hidden",
      }}>

        {/* Header */}
        <div style={{
          display:        "flex",
          alignItems:     "center",
          justifyContent: "space-between",
          padding:        "12px 16px",
          borderBottom:   `0.5px solid ${C.bdr}`,
          flexShrink:     0,
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <IcFilm size={18} color={C.acc}/>
            <div>
              <div style={{ fontSize: 13, fontWeight: 600, color: C.txt }}>
                Timelapse — {index}
              </div>
              <div style={{ fontSize: 10, color: C.dim }}>
                {source}{period ? ` · ${period}` : ""}
              </div>
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              background: "none", border: "none",
              color: C.dim, cursor: "pointer",
              display: "flex", padding: "0 4px",
            }}
          ><IcX size={18}/></button>
        </div>

        {/* GIF */}
        <div style={{
          flex:           1,
          overflowY:      "auto",
          display:        "flex",
          alignItems:     "center",
          justifyContent: "center",
          padding:        "12px",
          background:     C.bg,
          minHeight:      200,
        }}>
          <img
            ref={imgRef}
            src={gifSrc}
            alt={`Timelapse ${dataset} ${index}`}
            style={{
              maxWidth:     "100%",
              maxHeight:    "60vh",
              borderRadius: 6,
              border:       `1px solid ${C.bdr}`,
              display:      "block",
            }}
            onError={(e) => {
              e.target.style.display = "none";
              e.target.nextSibling.style.display = "flex";
            }}
          />
          {/* Fallback si le GIF ne charge pas */}
          <div style={{
            display:        "none",
            flexDirection:  "column",
            alignItems:     "center",
            gap:            8,
            color:          C.dim,
            fontSize:       12,
            padding:        24,
          }}>
            <IcAlert size={30} color={C.amb}/>
            <span>Impossible de charger le GIF</span>
            <a href={gifSrc} target="_blank" rel="noreferrer"
               style={{ color: C.acc, textDecoration: "underline", fontSize: 11 }}>
              Ouvrir directement
            </a>
          </div>
        </div>

        {/* Métadonnées + actions */}
        <div style={{
          padding:      "10px 16px",
          borderTop:    `0.5px solid ${C.bdr}`,
          flexShrink:   0,
          display:      "flex",
          alignItems:   "center",
          gap:          12,
        }}>
          {/* Stats */}
          <div style={{ flex: 1, display: "flex", gap: 12, flexWrap: "wrap" }}>
            {[
              { label: "Images",  value: `${frames}` },
              { label: "Dataset", value: dataset },
              sizeMb && { label: "Taille", value: sizeMb },
            ].filter(Boolean).map(({ label, value }) => (
              <div key={label} style={{ fontSize: 10, color: C.dim }}>
                <span style={{ color: C.mut, marginRight: 3 }}>{label}</span>
                <span style={{ color: C.txt, fontFamily: "monospace" }}>{value}</span>
              </div>
            ))}
          </div>

          {/* Boutons */}
          <div style={{ display: "flex", gap: 6, flexShrink: 0 }}>
            <button
              onClick={handleDownload}
              style={{
                fontFamily: F, fontSize: 11, padding: "6px 12px",
                borderRadius: 6, cursor: "pointer",
                background: C.acc, color: "#fff", border: "none",
                display: "flex", alignItems: "center", gap: 5,
              }}
            >
              <IcFileDown size={13}/> Télécharger
            </button>
            <button
              onClick={() => window.open(gifSrc, "_blank")}
              style={{
                fontFamily: F, fontSize: 11, padding: "6px 12px",
                borderRadius: 6, cursor: "pointer",
                background: "transparent", color: C.dim,
                border: `0.5px solid ${C.bdr}`,
              }}
            >
              ↗ Ouvrir
            </button>
          </div>
        </div>
      </div>
    </>
  );
}
