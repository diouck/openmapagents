/**
 * SplatViewerModal.jsx — Visualiseur Gaussian Splatting plein écran.
 *
 * Charge un nuage de splats (.ply / .splat / .ksplat) via
 * @mkkellogg/gaussian-splats-3d (Three.js). Visualiseur autonome (orbital),
 * non géoréférencé sur la carte. Librairies importées dynamiquement.
 */
import { useEffect, useRef, useState } from "react";
import { useThemeContext } from "../theme";
import { IcSparkles, IcX } from "../icons";

export default function SplatViewerModal({ url, onClose }) {
  const C = useThemeContext();
  const containerRef = useRef(null);
  const viewerRef = useRef(null);
  const [status, setStatus] = useState("Chargement du moteur 3D…");

  useEffect(() => {
    let disposed = false;
    (async () => {
      try {
        const GS = await import("@mkkellogg/gaussian-splats-3d");
        if (disposed || !containerRef.current) return;
        setStatus("Initialisation du visualiseur…");
        const viewer = new GS.Viewer({
          rootElement: containerRef.current,
          sharedMemoryForWorkers: false,   // évite l'exigence COOP/COEP
          useBuiltInControls: true,
          dynamicScene: false,
        });
        viewerRef.current = viewer;
        setStatus("Téléchargement du splat…");
        await viewer.addSplatScene(url, {
          showLoadingUI: true,
          splatAlphaRemovalThreshold: 5,
          progressiveLoad: true,
        });
        if (disposed) return;
        viewer.start();
        setStatus(null);
      } catch (e) {
        if (!disposed) setStatus(`Erreur : ${e?.message || e}. Avez-vous lancé « npm install » ?`);
      }
    })();

    return () => {
      disposed = true;
      try {
        const v = viewerRef.current;
        if (v) { v.stop?.(); v.dispose?.(); }
      } catch (_) {}
      viewerRef.current = null;
    };
  }, [url]);

  return (
    <div style={{ position: "fixed", inset: 0, zIndex: 9999, background: "#08080aee", display: "flex", flexDirection: "column" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "8px 12px", background: C.card, borderBottom: `1px solid ${C.bdr}` }}>
        <span style={{ fontSize: 13, fontWeight: 600, color: C.txt, display: "flex", alignItems: "center", gap: 6 }}><IcSparkles size={14}/> Gaussian Splat — {url.split("/").pop()}</span>
        <span style={{ fontSize: 11, color: status ? C.amb : C.dim, marginLeft: "auto" }}>
          {status || "Prêt — glisser pour orbiter, molette pour zoomer"}
        </span>
        <button onClick={onClose} style={{ fontSize: 12, padding: "4px 12px", borderRadius: 6, background: C.acc, color: "#fff", border: "none", cursor: "pointer", display: "flex", alignItems: "center", gap: 5 }}>
          Fermer <IcX size={13}/>
        </button>
      </div>
      <div ref={containerRef} style={{ flex: 1, position: "relative", overflow: "hidden" }} />
    </div>
  );
}
