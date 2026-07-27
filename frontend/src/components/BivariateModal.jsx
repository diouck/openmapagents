/**
 * BivariateModal.jsx — Fenêtre autonome pour la carte bivariée.
 *
 * Ouverte depuis le menu thématique (Outils → « Carte bivariée »). Enveloppe le
 * BivariatePanel existant dans une FloatingWindow (déplaçable, redimensionnable
 * sur tous les bords, superposable et passant au-dessus au clic).
 */
import FloatingWindow from "./FloatingWindow";
import BivariatePanel from "./BivariatePanel";
import { IcGrid } from "../icons";

export default function BivariateModal({ mapRef, layers = [], onAddRasterLayer, onClose, z, onFocus, initialPos }) {
  return (
    <FloatingWindow
      title="Carte bivariée" subtitle="Croise 2 variables en une matrice 3×3" icon={<IcGrid size={18}/>}
      z={z} onFocus={onFocus} onClose={onClose}
      initialPos={initialPos} initialSize={{ w: 330, h: 540 }} minW={300} minH={320}
      bodyStyle={{ padding: 0, display: "flex", flexDirection: "column" }}
    >
      <BivariatePanel
        mapRef={mapRef}
        layers={layers}
        onAddRasterLayer={onAddRasterLayer}
        geeReady={true}
      />
    </FloatingWindow>
  );
}
