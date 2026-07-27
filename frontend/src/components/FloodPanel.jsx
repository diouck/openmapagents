/**
 * FloodPanel.jsx — Cartographie des inondations.
 *
 * Deux voies (POST /api/gee/flood) :
 *   • SAR    — Sentinel-1 avant/après (radar, traverse les nuages).
 *   • Modèle — HAND (MERIT Hydro) piloté par un CURSEUR de hauteur d'eau.
 * (L'optique Sentinel-2 a été retirée : trop dépendante des nuages, la crue
 *  brève est diluée par le compositing et l'eau turbide reste invisible.)
 * Sorties : emprise inondée, surface, population exposée + surface bâtie (GHSL).
 *
 * Panneau autonome. Les couches (« Fond satellite » + « Zone inondée » raster,
 * « Périmètre inondation » vecteur) sont remplacées à chaque relance.
 */
import { useState } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { Sel, Lbl } from "./ui";
import {
  IcDroplets, IcRadar, IcMountain, IcAlert, IcCheck, IcInfo,
  IcUser, IcBuilding, IcWaves, IcCalendar,
} from "../icons";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";
const LNAMES = ["Fond satellite", "Zone inondée", "Périmètre inondation"];
const iso = d => d.toISOString().slice(0, 10);
const shift = (d, n) => { const x = new Date(d); x.setDate(x.getDate() + n); return x; };

const MODES = [
  ["sar",  "SAR (radar)",  IcRadar,    "Sentinel-1, traverse les nuages — détection d'un événement"],
  ["hand", "Modèle (MNT)", IcMountain, "Hauteur d'eau sur le terrain (HAND)"],
];

export default function FloodPanel({ layers, mapRef, onAddRasterLayer, onAddLayer, onRemoveLayers }) {
  const C = useThemeContext();
  const today = new Date();

  const [tab, setTab]     = useState("outil");
  const [mode, setMode]   = useState("sar");
  const [roi, setRoi]     = useState("map");
  const [preStart,  setPreStart]  = useState(iso(shift(today, -40)));
  const [preEnd,    setPreEnd]    = useState(iso(shift(today, -25)));
  const [postStart, setPostStart] = useState(iso(shift(today, -12)));
  const [postEnd,   setPostEnd]   = useState(iso(today));
  const [height, setHeight] = useState(2);          // curseur hauteur d'eau (m)
  const [sens, setSens] = useState(50);             // sensibilité de détection (SAR)
  const [base, setBase] = useState(true);           // fond satellite fausse couleur (contexte)
  const [busy, setBusy] = useState(false);
  const [err, setErr]   = useState(null);
  const [res, setRes]   = useState(null);

  const roiLayers = (layers || []).filter(l => !l.isRaster && l.geojson);

  const bbox = () => {
    const m = mapRef?.current?.getMap?.(); if (!m) return null;
    try {
      const b = m.getBounds();
      let w = Math.max(-180, b.getWest()), e = Math.min(180, b.getEast());
      let s = Math.max(-85, b.getSouth()), n = Math.min(85, b.getNorth());
      if (e <= w) { w = -179.9; e = 179.9; }
      return [w, s, e, n];
    } catch (_) { return null; }
  };

  const body = () => {
    const b = { mode, water_height: height, sensitivity: sens, context: base,
                pre_start: preStart, pre_end: preEnd, post_start: postStart, post_end: postEnd };
    if (roi !== "map") {
      const l = roiLayers.find(x => x.id === roi);
      const g = l?.geojson;
      if (g) { b.roi_geojson = g; b.bbox = l.bbox || bbox(); return b; }
    }
    b.bbox = bbox();
    return b;
  };

  const run = async () => {
    const b = body();
    if (!b.bbox && !b.roi_geojson) { setErr("Emprise introuvable — cadrez la carte."); return; }
    setErr(null); setBusy(true);
    onRemoveLayers?.(LNAMES);   // remplace la carte d'inondation précédente
    try {
      const r = await fetch(`${API}/api/gee/flood`, {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(b),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || `Erreur ${r.status}`);
      setRes(d);
      if (d.context_url) {   // fond fausse couleur SOUS la nappe (ajouté en premier)
        onAddRasterLayer?.({ id: "flood_context", name: "Fond satellite", tileUrl: d.context_url,
          type: "wms", opacity: 1, visParams: d.context_vis, bbox: b.bbox });
      }
      onAddRasterLayer?.({ id: "flood_extent", name: "Zone inondée", tileUrl: d.tile_url,
        type: "wms", opacity: 0.85, visParams: d.vis_params, bbox: b.bbox });
      if (d.perimeter) {
        onAddLayer?.({ type: "FeatureCollection", features: [{ type: "Feature",
          properties: { surface_ha: d.surface_ha }, geometry: d.perimeter }] },
          "Périmètre inondation", "analysis");
      }
    } catch (e) { setErr(e.message); }
    setBusy(false);
  };

  // ── Styles ───────────────────────────────────────────────────
  const box = { background: C.bg, borderRadius: 8, padding: 10, border: `0.5px solid ${C.bdr}`,
                display: "flex", flexDirection: "column", gap: 8 };
  const dat = { fontFamily: M, fontSize: 10.5, padding: "5px 7px", borderRadius: 5, width: "100%",
                background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", boxSizing: "border-box" };
  const h = { fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 4 };
  const p = { fontSize: 11, color: C.mut, lineHeight: 1.6, margin: 0 };
  const num = (v, u = "") => (v === undefined || v === null) ? "—" : `${v.toLocaleString("fr")}${u}`;

  const dateWin = (k) => {
    const [d0, d1] = k === "pre" ? [preStart, preEnd] : [postStart, postEnd];
    const s0 = k === "pre" ? setPreStart : setPostStart;
    const s1 = k === "pre" ? setPreEnd : setPostEnd;
    return (
      <div style={box}>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <IcCalendar size={12} style={{ color: C.dim }} />
          <span style={{ fontSize: 10.5, fontWeight: 500, color: C.txt }}>{k === "pre" ? "Avant l'inondation" : "Après l'inondation"}</span>
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          <input type="date" value={d0} onChange={e => s0(e.target.value)} style={dat} />
          <input type="date" value={d1} onChange={e => s1(e.target.value)} style={dat} />
        </div>
      </div>
    );
  };

  return (
    <div style={{ padding: 12, display: "flex", flexDirection: "column", gap: 10, overflowY: "auto" }}>

      <div style={{ display: "flex", gap: 4, borderBottom: `0.5px solid ${C.bdr}` }}>
        {[["outil", "Inondation"], ["info", "Définition"]].map(([k, l]) => (
          <button key={k} onClick={() => setTab(k)} style={{
            flex: 1, fontFamily: F, fontSize: 11.5, fontWeight: tab === k ? 600 : 400, padding: "6px 2px",
            cursor: "pointer", background: "transparent", border: "none",
            borderBottom: `2px solid ${tab === k ? C.acc : "transparent"}`, color: tab === k ? C.acc : C.dim,
            marginBottom: -0.5, display: "flex", alignItems: "center", justifyContent: "center", gap: 4,
          }}>{k === "info" && <IcInfo size={12} />}{l}</button>
        ))}
      </div>

      {tab === "outil" && (<>
        <div style={{ fontSize: 10.5, color: C.dim, lineHeight: 1.5 }}>
          Cartographie les zones inondées et estime les impacts. Détection satellite d'un événement,
          ou simulation par hauteur d'eau sur le terrain.
        </div>

        {/* Méthode */}
        <div style={box}>
          <Lbl>Méthode</Lbl>
          {MODES.map(([k, label, Ic, desc]) => (
            <button key={k} onClick={() => setMode(k)} style={{
              display: "flex", alignItems: "flex-start", gap: 8, width: "100%", textAlign: "left",
              padding: "7px 9px", borderRadius: 7, cursor: "pointer",
              background: mode === k ? C.acc + "14" : "transparent",
              border: mode === k ? `2px solid ${C.acc}` : `0.5px solid ${C.bdr}`,
            }}>
              <Ic size={15} style={{ color: mode === k ? C.acc : C.mut, flexShrink: 0, marginTop: 1 }} />
              <span>
                <span style={{ display: "block", fontSize: 11.5, fontWeight: 500, color: C.txt }}>{label}</span>
                <span style={{ display: "block", fontSize: 9.5, color: C.dim, lineHeight: 1.4 }}>{desc}</span>
              </span>
            </button>
          ))}
        </div>

        {/* Emprise */}
        <div style={box}>
          <Lbl>Emprise</Lbl>
          <Sel value={roi} onChange={setRoi} options={[
            { value: "map", label: "Vue carte actuelle" },
            ...roiLayers.map(l => ({ value: l.id, label: `Couche · ${l.name}` })),
          ]} />
          {mode !== "hand" && (
            <label style={{ display: "flex", alignItems: "center", gap: 7, cursor: "pointer", fontSize: 10.5, color: C.mut }}>
              <input type="checkbox" checked={base} onChange={e => setBase(e.target.checked)}
                style={{ accentColor: C.acc, width: 13, height: 13, flexShrink: 0 }} />
              Fond satellite fausse couleur <span style={{ fontSize: 9, color: C.dim }}>— nappe en rouge par-dessus</span>
            </label>
          )}
        </div>

        {/* Dates (SAR) OU curseur (modèle) */}
        {mode !== "hand" ? (
          <>
            {dateWin("pre")}{dateWin("post")}
            <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.45, paddingLeft: 2 }}>
              Sentinel-1 repasse tous les ~6-12 jours ; gardez des fenêtres d'environ
              deux semaines. La fin de fenêtre est incluse, et une fenêtre vide est
              élargie automatiquement.
            </div>

            <div style={box}>
              <Lbl>Sensibilité de détection</Lbl>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 9, color: C.dim }}>stricte</span>
                <input type="range" min="0" max="100" step="5" value={sens}
                  onChange={e => setSens(parseInt(e.target.value))} style={{ flex: 1, height: 3, accentColor: C.acc }} />
                <span style={{ fontSize: 9, color: C.dim }}>large</span>
                <span style={{ fontFamily: M, fontSize: 10.5, color: C.txt, width: 30, textAlign: "right" }}>{sens}</span>
              </div>
              <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.45 }}>
                Monter la sensibilité capte les <b style={{ color: C.mut }}>marges turbides et l'eau
                peu profonde</b> (souvent manquées), au prix de quelques faux positifs. Ajustez jusqu'à
                coller à l'emprise visible.
              </div>
            </div>
          </>
        ) : (
          <div style={box}>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <IcWaves size={13} style={{ color: C.acc }} />
              <Lbl>Hauteur d'eau simulée</Lbl>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
              <input type="range" min="0.5" max="15" step="0.5" value={height}
                onChange={e => setHeight(parseFloat(e.target.value))}
                onMouseUp={() => run()} onTouchEnd={() => run()}
                style={{ flex: 1, height: 4, accentColor: C.acc }} />
              <span style={{ fontFamily: M, fontSize: 14, fontWeight: 700, color: C.txt, width: 54, textAlign: "right" }}>
                {height} m
              </span>
            </div>
            <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.45 }}>
              Inonde le terrain là où la hauteur au-dessus du cours d'eau le plus proche (HAND)
              est inférieure à cette valeur. Relâcher le curseur relance la carte.
            </div>
          </div>
        )}

        {err && (
          <div style={{ display: "flex", gap: 6, fontSize: 10.5, color: C.red, background: C.red + "12",
                        borderRadius: 6, padding: "7px 9px" }}>
            <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} /> {err}
          </div>
        )}

        <button onClick={run} disabled={busy} style={{
          fontFamily: F, fontSize: 11.5, fontWeight: 600, padding: "9px 0", borderRadius: 7,
          background: busy ? C.hover : C.acc, color: busy ? C.dim : "#fff", border: "none",
          cursor: busy ? "default" : "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
        }}>
          <IcDroplets size={14} /> {busy ? "Calcul GEE en cours…" : "Cartographier l'inondation"}
        </button>

        {/* Résultats */}
        {res && (
          <div style={{ ...box, borderColor: C.acc + "44", background: C.acc + "0a" }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 7 }}>
              <IcCheck size={13} style={{ color: C.acc }} />
              <span style={{ fontFamily: M, fontSize: 17, fontWeight: 700, color: C.txt }}>
                {num(res.surface_km2)} <span style={{ fontSize: 11, fontWeight: 400, color: C.mut }}>km²</span>
              </span>
              <span style={{ fontSize: 10.5, color: C.mut }}>inondés · {num(res.surface_ha, " ha")}</span>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 3, marginTop: 2 }}>
              {[["Population exposée", res.population != null ? num(res.population, " hab.") : null, IcUser],
                ["Surface bâtie inondée", res.bati_ha != null ? num(res.bati_ha, " ha") : null, IcBuilding]]
                .filter(([, v]) => v != null).map(([k, v, Ic]) => (
                  <div key={k} style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 10.5 }}>
                    <Ic size={12} style={{ color: C.dim, flexShrink: 0 }} />
                    <span style={{ flex: 1, color: C.dim }}>{k}</span>
                    <span style={{ fontFamily: M, fontSize: 10.5, color: C.txt }}>{v}</span>
                  </div>
                ))}
            </div>
            {res.notes?.length > 0 && (
              <div style={{ display: "flex", gap: 6, fontSize: 9, color: C.dim, lineHeight: 1.45,
                            borderTop: `0.5px solid ${C.bdr}`, paddingTop: 6, marginTop: 2 }}>
                <IcInfo size={11} style={{ flexShrink: 0, marginTop: 1 }} /><span>{res.notes.join(" ")}</span>
              </div>
            )}
          </div>
        )}
      </>)}

      {tab === "info" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div>
            <div style={h}>Cartographie des inondations</div>
            <p style={p}>
              Deux logiques : <b style={{ color: C.mut }}>détecter</b> une inondation réelle sur
              images satellite (avant/après), ou la <b style={{ color: C.mut }}>simuler</b> à partir
              du terrain pour une hauteur d'eau donnée.
            </p>
          </div>
          <div>
            <div style={h}>Méthodes</div>
            <p style={{ ...p, fontSize: 10.5 }}>
              <b style={{ color: C.mut }}>SAR (Sentinel-1)</b> : le radar traverse les nuages — décisif
              pendant une tempête. L'eau lisse renvoie peu de signal ; une chute de rétrodiffusion
              entre avant et après révèle l'eau nouvelle. C'est la méthode de Copernicus EMS. La
              nappe brute est ensuite recollée (fermeture morphologique) pour une emprise continue,
              et le curseur de sensibilité règle l'ampleur de la détection.
            </p>
            <p style={{ ...p, fontSize: 10.5, marginTop: 4 }}>
              <b style={{ color: C.mut }}>Modèle (HAND)</b> : on inonde le terrain là où la hauteur
              au-dessus du cours d'eau le plus proche est inférieure au curseur. Physique (l'eau
              déborde des rivières), contrairement à un simple seuil d'altitude qui noierait des
              cuvettes isolées.
            </p>
            <p style={{ ...p, fontSize: 10, marginTop: 4, color: C.dim }}>
              <b style={{ color: C.mut }}>Optique (Sentinel-2) — retirée.</b> Un indice d'eau
              avant/après paraît simple, mais reste aveugle sous les nuages de la tempête, dilue la
              crue brève lors du compositing et manque l'eau turbide (couleur du sable). Peu fiable
              en pratique : on s'appuie sur le radar.
            </p>
          </div>
          <div>
            <div style={h}>Rendu</div>
            <p style={{ ...p, fontSize: 10.5 }}>
              La nappe inondée est peinte en <b style={{ color: "#e11d1d" }}>rouge</b> sur un
              <b style={{ color: C.mut }}> fond satellite fausse couleur SWIR</b> (Sentinel-2, comme
              Copernicus EMS) : végétation verte, sols nus rose-gris, eau sombre. Décochez le fond
              pour ne garder que la nappe sur votre carte.
            </p>
          </div>
          <div style={{ display: "flex", gap: 8, background: C.amb + "12",
                        border: `0.5px solid ${C.amb}33`, borderRadius: 7, padding: "8px 10px" }}>
            <span style={{ color: C.amb, flexShrink: 0, marginTop: 1 }}><IcAlert size={13} /></span>
            <div>
              <div style={{ fontSize: 9.5, color: C.amb, fontWeight: 600, marginBottom: 2 }}>À SAVOIR</div>
              <p style={{ ...p, fontSize: 10.5 }}>
                On retire l'eau permanente (JRC), les pentes fortes et les pixels isolés. La
                population et le bâti viennent de GHSL (~modélisés, 100 m). Le modèle HAND est à
                ~90 m : bon pour le régional, pas pour la parcelle urbaine.
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
