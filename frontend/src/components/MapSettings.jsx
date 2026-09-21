/**
 * MapSettings.jsx — Panneau ⚙️ « Contrôles » flottant sur la carte.
 *
 * Trois sections, tout désactivable / cliquable au même endroit :
 *   1. Fonds de carte  — vignettes cliquables (miniatures) des fonds disponibles.
 *   2. Contrôles carte — interrupteurs des interactions & contrôles natifs
 *      (zoom, rotation/inclinaison, déplacement, boutons zoom, échelle, géoloc,
 *       plein écran) — API commune MapLibre / Mapbox.
 *   3. Outils          — activer / désactiver les outils de l'app (édition,
 *      tampon, mesure, dessin).
 *
 * Le composant ne fait QUE piloter l'état (remonté à App via props) : App applique
 * les handlers sur la carte et rend/masque les contrôles natifs.
 */
import { F, MAPBOX_TOKEN } from "../config";
import { MAP_ENGINE } from "../mapgl";
import { IcX, IcStack, IcSettings, IcEdit, IcCircleDot, IcRulerTool, IcPencil, IcAlert } from "../icons";

// Vignettes : les styles vecteur (OpenFreeMap) n'ont pas d'image statique → on
// prend une tuile raster équivalente (CARTO/OSM) au même endroit. Les styles raster
// (satellite, planètes) fournissent eux-mêmes leur miniature via leurs tuiles. Les
// styles Mapbox natifs (mapbox://) → tuile via l'API statique Mapbox (avec token).
const EARTH_THUMB = {
  positron: "https://a.basemaps.cartocdn.com/light_all/4/8/5.png",
  dark:     "https://a.basemaps.cartocdn.com/dark_all/4/8/5.png",
  liberty:  "https://tile.openstreetmap.org/4/8/5.png",
};
function thumbFor(key, style) {
  if (typeof style === "string" && style.startsWith("mapbox://styles/")) {
    if (!MAPBOX_TOKEN) return null;
    const path = style.replace("mapbox://styles/", "");   // owner/style-id
    return `https://api.mapbox.com/styles/v1/${path}/tiles/256/2/2/1?access_token=${MAPBOX_TOKEN}`;
  }
  if (EARTH_THUMB[key]) return EARTH_THUMB[key];
  if (style && typeof style === "object" && style.sources) {
    const s = Object.values(style.sources).find(v => v && v.type === "raster" && v.tiles && v.tiles[0]);
    if (s) return s.tiles[0].replace("{s}", "a").replace("{z}", "2").replace("{x}", "2").replace("{y}", "1");
  }
  return null;
}

export default function MapSettings({ C, mapStyles, planetKeys, mapSt, onBasemap,
                                      ctrls, setCtrls, activeTool, onTool, onClose }) {
  const set = (k) => setCtrls(p => ({ ...p, [k]: !p[k] }));

  const earthKeys  = Object.keys(mapStyles).filter(k => !planetKeys.includes(k));
  const planetOpts = Object.keys(mapStyles).filter(k => planetKeys.includes(k));

  const CTRL_ROWS = [
    ["zoom",       "Zoom (molette, double-clic, boutons)"],
    ["rotate",     "Rotation / inclinaison"],
    ["pan",        "Déplacement (glisser)"],
    ["nav",        "Boutons zoom & boussole"],
    ["search",     "Recherche (adresse)"],
    ["scale",      "Échelle"],
    ["geolocate",  "Géolocalisation"],
    ["fullscreen", "Plein écran"],
  ];
  const TOOLS = [
    ["editor",  "Édition", IcEdit],
    ["buffer",  "Tampon",  IcCircleDot],
    ["measure_dist", "Mesure", IcRulerTool],
    ["draw",    "Dessin",  IcPencil],
  ];

  const lbl = { fontSize: 11, color: C.txt };
  const secTitle = { fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".06em",
                     margin: "2px 0 6px", display: "flex", alignItems: "center", gap: 5 };

  // Interrupteur compact (pilule)
  const Switch = ({ on, onClick }) => (
    <button onClick={onClick} aria-pressed={on} style={{
      width: 30, height: 17, borderRadius: 10, border: "none", cursor: "pointer", flexShrink: 0,
      background: on ? C.acc : C.bdr, position: "relative", transition: "background .15s", padding: 0,
    }}>
      <span style={{ position: "absolute", top: 2, left: on ? 15 : 2, width: 13, height: 13, borderRadius: "50%",
                     background: "#fff", transition: "left .15s", boxShadow: "0 1px 2px rgba(0,0,0,.3)" }} />
    </button>
  );

  return (
    <div style={{
      position: "absolute", top: 52, right: 10, zIndex: 2900, width: 244, maxWidth: "calc(100vw - 24px)",
      maxHeight: "calc(100vh - 130px)", overflowY: "auto",
      background: C.panel || C.bg, border: `0.5px solid ${C.bdr}`, borderRadius: 10,
      boxShadow: "0 8px 30px rgba(0,0,0,.28)", padding: 12,
      display: "flex", flexDirection: "column", gap: 12, fontFamily: F,
    }}>
      {/* En-tête */}
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <IcSettings size={14} style={{ color: C.acc }} />
        <span style={{ fontSize: 12, fontWeight: 700, color: C.txt, flex: 1 }}>Contrôles</span>
        <button onClick={onClose} title="Fermer" style={{ background: "none", border: "none", cursor: "pointer", color: C.dim, padding: 2, display: "flex" }}>
          <IcX size={14} />
        </button>
      </div>

      {/* ── Fonds de carte (vignettes) ── */}
      <div>
        <div style={secTitle}><IcStack size={11} /> Fonds de carte</div>
        {MAP_ENGINE === "mapbox" && !MAPBOX_TOKEN && (
          <div style={{ display: "flex", gap: 6, fontSize: 9.5, color: C.amb, background: C.amb + "14",
                        border: `0.5px solid ${C.amb}44`, borderRadius: 6, padding: "6px 8px", marginBottom: 8, lineHeight: 1.4 }}>
            <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} />
            <span>Les fonds Mapbox exigent un <b>token</b>. Ajoutez <b>VITE_MAPBOX_TOKEN</b> puis rebuild, ou repassez sur MapLibre.</span>
          </div>
        )}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 6 }}>
          {[...earthKeys, ...planetOpts].map(k => {
            const active = mapSt === k;
            const thumb = thumbFor(k, mapStyles[k]);
            return (
              <button key={k} onClick={() => onBasemap(k)} title={k}
                style={{ display: "flex", flexDirection: "column", gap: 3, padding: 3, cursor: "pointer",
                         border: `1.5px solid ${active ? C.acc : "transparent"}`, borderRadius: 7,
                         background: active ? C.acc + "12" : "transparent" }}>
                <div style={{ width: "100%", height: 40, borderRadius: 4, overflow: "hidden",
                              border: `0.5px solid ${C.bdr}`, background: C.input,
                              backgroundImage: thumb ? `url(${thumb})` : "none",
                              backgroundSize: "cover", backgroundPosition: "center",
                              display: "flex", alignItems: "center", justifyContent: "center" }}>
                  {!thumb && <span style={{ fontSize: 9, color: C.dim }}>{k}</span>}
                </div>
                <span style={{ fontSize: 8.5, color: active ? C.acc : C.mut, textAlign: "center",
                               overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                               textTransform: "capitalize" }}>{k}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* ── Contrôles carte (interrupteurs) ── */}
      <div>
        <div style={secTitle}><IcSettings size={11} /> Contrôles carte</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 7 }}>
          {CTRL_ROWS.map(([k, l]) => (
            <div key={k} style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ ...lbl, flex: 1 }}>{l}</span>
              <Switch on={!!ctrls[k]} onClick={() => set(k)} />
            </div>
          ))}
        </div>
      </div>

      {/* ── Outils (activer / désactiver) ── */}
      <div>
        <div style={secTitle}><IcEdit size={11} /> Outils</div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 6 }}>
          {TOOLS.map(([id, l, Ic]) => {
            const on = activeTool === id;
            return (
              <button key={id} onClick={() => onTool(on ? "pointer" : id)}
                style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
                         fontFamily: F, fontSize: 10.5, padding: "7px 6px", borderRadius: 6, cursor: "pointer",
                         border: `0.5px solid ${on ? C.acc + "66" : C.bdr}`,
                         background: on ? C.acc + "15" : "transparent", color: on ? C.acc : C.mut }}>
                <Ic size={12} /> {l}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
