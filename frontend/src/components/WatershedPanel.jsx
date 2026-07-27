/**
 * WatershedPanel.jsx — Délimitation de bassin versant depuis un exutoire.
 *
 * L'utilisateur pose un point exutoire (clic carte), le backend délimite le
 * bassin qui draine vers ce point via HydroSHEDS (HydroBASINS + remontée de
 * l'amont par la topologie NEXT_DOWN), et renvoie limite + réseau + attributs.
 *
 * Traitement en deux temps : /watershed (limite + réseau, rapide) puis
 * /watershed/attributes (sol, relief, climat, nappe). Les couches d'un run
 * précédent sont retirées à chaque relance (onRemoveLayers). Clic capté par
 * `map.once`, sans toucher au clic central de la carte.
 */
import { useState } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { Sel, Lbl } from "./ui";
import {
  IcDroplets, IcMapPin, IcAlert, IcCheck, IcInfo, IcCrosshair, IcLoader,
  IcMountain, IcWaves, IcLandPlot, IcCloudRain, IcSpline, IcExternalLink,
} from "../icons";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";
const WS_NAMES = ["Bassin versant", "Réseau hydrographique", "Exutoire"];
const STEPS = [
  ["delin", "Délimitation du bassin (HydroSHEDS)"],
  ["net",   "Extraction du réseau hydrographique"],
  ["attr",  "Attributs : sol, relief, climat, nappe"],
];

export default function WatershedPanel({ layers, mapRef, onAddLayer, onAddLayerSilent, onRemoveLayers }) {
  const C = useThemeContext();

  const [tab, setTab]         = useState("outil");   // outil | info
  const [outlet, setOutlet]   = useState(null);      // { lat, lon }
  const [picking, setPicking] = useState(false);
  const [level, setLevel]     = useState(12);
  const [upstream, setUpstream] = useState(true);
  const [step, setStep]       = useState(null);      // clé d'étape en cours, ou "done"
  const [busy, setBusy]       = useState(false);
  const [err, setErr]         = useState(null);
  const [res, setRes]         = useState(null);       // { attributes, notes, unavailable }

  const mapObj = () => mapRef?.current?.getMap?.() || null;

  const pick = () => {
    const m = mapObj();
    if (!m) { setErr("Carte indisponible."); return; }
    setErr(null);
    if (picking) { setPicking(false); m.getCanvas().style.cursor = ""; return; }
    setPicking(true);
    m.getCanvas().style.cursor = "crosshair";
    m.once("click", (e) => {
      setOutlet({ lat: +e.lngLat.lat.toFixed(5), lon: +e.lngLat.lng.toFixed(5) });
      setPicking(false); m.getCanvas().style.cursor = "";
    });
  };

  const setLat = (v) => setOutlet(o => ({ lat: parseFloat(v), lon: o?.lon ?? 0 }));
  const setLon = (v) => setOutlet(o => ({ lat: o?.lat ?? 0, lon: parseFloat(v) }));

  const run = async () => {
    if (!outlet || Number.isNaN(outlet.lat) || Number.isNaN(outlet.lon)) {
      setErr("Placez d'abord un exutoire (clic sur la carte ou coordonnées).");
      return;
    }
    setErr(null); setRes(null); setBusy(true); setStep("delin");
    onRemoveLayers?.(WS_NAMES);   // remplace le bassin précédent au lieu de l'empiler

    let delim;
    try {
      const r = await fetch(`${API}/api/gee/watershed`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ lat: outlet.lat, lon: outlet.lon, level, include_upstream: upstream }),
      });
      delim = await r.json();
      if (!r.ok) throw new Error(delim.detail || `Erreur ${r.status}`);
    } catch (e) { setErr(e.message); setBusy(false); setStep(null); return; }

    // Étape 1 finie → on affiche déjà limite + réseau + exutoire.
    setStep("net");
    const A0 = delim.attributes || {};
    if (delim.boundary) {
      onAddLayer?.({ type: "FeatureCollection", features: [{
        type: "Feature", geometry: delim.boundary,
        properties: { nom: "Bassin versant", surface_km2: A0.surface_km2, perimetre_km: A0.perimetre_km,
                      reseau_km: A0.reseau_km, sous_bassins: A0.sous_bassins },
      }] }, "Bassin versant", "analysis");
    }
    if (delim.rivers?.features?.length) {
      onAddLayerSilent?.(delim.rivers, "Réseau hydrographique", "data",
        { color: "#2b83ba", opacity: 0.9, radius: 3 });
    }
    onAddLayerSilent?.({ type: "FeatureCollection", features: [{
      type: "Feature", geometry: { type: "Point", coordinates: [outlet.lon, outlet.lat] },
      properties: { type: "exutoire" },
    }] }, "Exutoire", "data", { color: "#e01e1e", radius: 7 });

    setRes({ attributes: A0, notes: delim.notes || [], unavailable: delim.unavailable || [] });

    // Étape 2 : attributs thématiques (échec non bloquant — le bassin reste utile).
    setStep("attr");
    try {
      const r2 = await fetch(`${API}/api/gee/watershed/attributes`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ boundary: delim.boundary }),
      });
      const at = await r2.json();
      if (r2.ok) {
        setRes(prev => ({
          attributes:  { ...prev.attributes, ...(at.attributes || {}) },
          notes:       [...(prev.notes || []), ...(at.notes || [])],
          unavailable: [...(prev.unavailable || []), ...(at.unavailable || [])],
        }));
      } else {
        setRes(prev => ({ ...prev, notes: [...(prev.notes || []), "Attributs thématiques indisponibles sur ce bassin."] }));
      }
    } catch (_) {
      setRes(prev => ({ ...prev, notes: [...(prev.notes || []), "Attributs thématiques indisponibles (réseau)."] }));
    }
    setStep("done"); setBusy(false);
  };

  // ── Styles ───────────────────────────────────────────────────
  const box = { background: C.bg, borderRadius: 8, padding: 10, border: `0.5px solid ${C.bdr}`,
                display: "flex", flexDirection: "column", gap: 8 };
  const dat = { fontFamily: M, fontSize: 10.5, padding: "5px 7px", borderRadius: 5, width: "100%",
                background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none",
                boxSizing: "border-box" };
  const h = { fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 4 };
  const p = { fontSize: 11, color: C.mut, lineHeight: 1.6, margin: 0 };

  const A = res?.attributes || {};
  const fnum = (v, u = "") => (v === undefined || v === null) ? null : `${v.toLocaleString("fr")}${u}`;

  const rows = [
    ["Surface",                fnum(A.surface_km2, " km²"),  IcLandPlot],
    ["Périmètre",              fnum(A.perimetre_km, " km"),  IcWaves],
    ["Sous-bassins agrégés",   fnum(A.sous_bassins),         IcDroplets],
    ["Réseau hydro (total)",   fnum(A.reseau_km, " km"),     IcSpline],
    ["dont permanent (proxy)", fnum(A.reseau_permanent_km, " km"),  IcSpline],
    ["dont temporaire (proxy)",fnum(A.reseau_temporaire_km, " km"), IcSpline],
    ["Altitude min / max",     (A.altitude_min != null) ? `${fnum(A.altitude_min)} – ${fnum(A.altitude_max, " m")}` : null, IcMountain],
    ["Altitude moyenne",       fnum(A.altitude_moy, " m"),   IcMountain],
    ["Pente moyenne",          fnum(A.pente_moy_deg, " °"),  IcMountain],
    ["Précipitation annuelle", fnum(A.precip_mm_an, " mm/an"), IcCloudRain],
    ["Occupation dominante",   A.occupation_dominante || null, IcLandPlot],
    ["Sol — argile",           A.sol ? fnum(A.sol.argile_pct, " %") : null,  IcLandPlot],
    ["Sol — carbone org.",     A.sol ? fnum(A.sol.carbone_gkg, " g/kg") : null, IcLandPlot],
    ["Sol — pH",               A.sol ? fnum(A.sol.ph) : null, IcLandPlot],
    ["Nappe (proxy GLDAS)",    fnum(A.nappe_proxy_mm, " mm"), IcDroplets],
  ].filter(([, v]) => v != null);

  return (
    <div style={{ padding: 12, display: "flex", flexDirection: "column", gap: 10, overflowY: "auto" }}>

      {/* Onglets */}
      <div style={{ display: "flex", gap: 4, borderBottom: `0.5px solid ${C.bdr}` }}>
        {[["outil", "Bassin versant"], ["info", "Définition"]].map(([k, l]) => (
          <button key={k} onClick={() => setTab(k)} style={{
            flex: 1, fontFamily: F, fontSize: 11.5, fontWeight: tab === k ? 600 : 400,
            padding: "6px 2px", cursor: "pointer", background: "transparent", border: "none",
            borderBottom: `2px solid ${tab === k ? C.acc : "transparent"}`,
            color: tab === k ? C.acc : C.dim, marginBottom: -0.5,
            display: "flex", alignItems: "center", justifyContent: "center", gap: 4,
          }}>
            {k === "info" && <IcInfo size={12} />}
            {l}
          </button>
        ))}
      </div>

      {tab === "outil" && (<>
        <div style={{ fontSize: 10.5, color: C.dim, lineHeight: 1.5 }}>
          Posez un <b style={{ color: C.mut }}>exutoire</b> sur la carte : le bassin versant qui draine
          vers ce point est délimité via HydroSHEDS, avec son réseau et ses attributs.
        </div>

        {/* Exutoire */}
        <div style={box}>
          <Lbl>Point exutoire</Lbl>
          <button onClick={pick} style={{
            fontFamily: F, fontSize: 11, fontWeight: 600, padding: "8px 0", borderRadius: 7,
            background: picking ? C.amb : C.acc + "18", color: picking ? "#fff" : C.acc,
            border: `0.5px solid ${picking ? C.amb : C.acc}66`, cursor: "pointer",
            display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
          }}>
            <IcCrosshair size={13} /> {picking ? "Cliquez sur la carte…" : "Placer l'exutoire sur la carte"}
          </button>
          <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <span style={{ fontSize: 9.5, color: C.dim, width: 26 }}>Lat</span>
            <input type="number" step="0.0001" value={outlet?.lat ?? ""} placeholder="latitude"
              onChange={e => setLat(e.target.value)} style={dat} />
            <span style={{ fontSize: 9.5, color: C.dim, width: 26 }}>Lon</span>
            <input type="number" step="0.0001" value={outlet?.lon ?? ""} placeholder="longitude"
              onChange={e => setLon(e.target.value)} style={dat} />
          </div>
          {outlet && !Number.isNaN(outlet.lat) && (
            <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, color: C.mut }}>
              <IcMapPin size={12} style={{ color: C.red }} /> {outlet.lat.toFixed(4)}°, {outlet.lon.toFixed(4)}°
            </div>
          )}
          <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.45 }}>
            Le bassin est tracé depuis ce point exact. Posez-le bien <b style={{ color: C.mut }}>sur
            le cours d'eau</b> visé : un point sur un grand fleuve renvoie tout son bassin amont.
          </div>
        </div>

        {/* Options */}
        <div style={box}>
          <Lbl>Niveau de délimitation</Lbl>
          <Sel value={String(level)} onChange={v => setLevel(parseInt(v))} options={[
            { value: "12", label: "Niveau 12 — sous-bassins fins (défaut)" },
            { value: "10", label: "Niveau 10 — bassins moyens" },
            { value: "8",  label: "Niveau 8 — grands bassins" },
          ]} />
          <div style={{ fontSize: 9.5, color: C.dim, lineHeight: 1.45 }}>
            HydroBASINS emboîte les sous-bassins du niveau 1 (méga-bassins) au niveau 12 (le plus
            fin) : <b style={{ color: C.mut }}>12</b> = contour précis ; <b style={{ color: C.mut }}>10 / 8</b>
            = unités plus grosses, contour plus grossier.
          </div>
          <label style={{ display: "flex", alignItems: "flex-start", gap: 7, cursor: "pointer" }}>
            <input type="checkbox" checked={upstream} onChange={e => setUpstream(e.target.checked)}
              style={{ accentColor: C.acc, marginTop: 2, cursor: "pointer" }} />
            <span>
              <span style={{ fontSize: 10.5, color: C.txt }}>Inclure tout l'amont</span>
              <span style={{ display: "block", fontSize: 9.5, color: C.dim, lineHeight: 1.45, marginTop: 1 }}>
                Le vrai bassin versant qui draine vers l'exutoire. Décoché : seulement le sous-bassin
                local contenant le point (instantané).
              </span>
            </span>
          </label>
        </div>

        {err && (
          <div style={{ display: "flex", gap: 6, fontSize: 10.5, color: C.red, background: C.red + "12",
                        borderRadius: 6, padding: "7px 9px" }}>
            <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} /> {err}
          </div>
        )}

        <button onClick={run} disabled={busy} style={{
          fontFamily: F, fontSize: 11.5, fontWeight: 600, padding: "9px 0", borderRadius: 7,
          background: busy ? C.hover : C.acc, color: busy ? C.dim : "#fff",
          border: "none", cursor: busy ? "default" : "pointer",
          display: "flex", alignItems: "center", justifyContent: "center", gap: 6,
        }}>
          <IcDroplets size={14} /> {busy ? "Délimitation en cours…" : "Délimiter le bassin versant"}
        </button>

        {/* Étapes de traitement */}
        {step && (
          <div style={{ ...box, gap: 6 }}>
            {STEPS.map(([k, label]) => {
              const order = STEPS.findIndex(s => s[0] === k);
              const cur = STEPS.findIndex(s => s[0] === step);
              const done = step === "done" || order < cur;
              const active = step === k;
              return (
                <div key={k} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 10.5,
                                      color: done ? C.acc : active ? C.txt : C.dim }}>
                  <span style={{ width: 14, display: "flex", justifyContent: "center", flexShrink: 0 }}>
                    {done ? <IcCheck size={12} />
                          : active ? <IcLoader size={12} className="ws-spin" />
                          : <span style={{ width: 5, height: 5, borderRadius: 5, background: C.bdr }} />}
                  </span>
                  {label}
                </div>
              );
            })}
          </div>
        )}

        {/* Résultats */}
        {res && (
          <div style={{ ...box, borderColor: C.acc + "44", background: C.acc + "0a" }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 7 }}>
              <IcCheck size={13} style={{ color: C.acc }} />
              <span style={{ fontFamily: M, fontSize: 17, fontWeight: 700, color: C.txt }}>
                {fnum(A.surface_km2) ?? "—"} <span style={{ fontSize: 11, fontWeight: 400, color: C.mut }}>km²</span>
              </span>
              {A.sous_bassins != null && (
                <span style={{ fontSize: 10.5, color: C.mut }}>· {fnum(A.sous_bassins)} sous-bassin(s)</span>
              )}
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: 2, marginTop: 2 }}>
              {rows.map(([k, v, Ic]) => (
                <div key={k} style={{ display: "flex", alignItems: "center", gap: 7, fontSize: 10.5, padding: "1px 0" }}>
                  {Ic && <Ic size={11} style={{ color: C.dim, flexShrink: 0 }} />}
                  <span style={{ flex: 1, minWidth: 0, color: C.dim, overflow: "hidden",
                                 textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{k}</span>
                  <span style={{ fontFamily: M, fontSize: 10, color: C.txt, flexShrink: 0 }}>{v}</span>
                </div>
              ))}
              {step === "attr" && (
                <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 9.5, color: C.dim, marginTop: 2 }}>
                  <IcLoader size={10} className="ws-spin" /> calcul des attributs thématiques…
                </div>
              )}
            </div>

            {res.notes?.length > 0 && (
              <div style={{ display: "flex", gap: 6, fontSize: 9, color: C.dim, lineHeight: 1.45,
                            borderTop: `0.5px solid ${C.bdr}`, paddingTop: 6, marginTop: 2 }}>
                <IcInfo size={11} style={{ flexShrink: 0, marginTop: 1 }} />
                <span>{res.notes.join(" ")}</span>
              </div>
            )}
            {res.unavailable?.length > 0 && (
              <div style={{ fontSize: 9, color: C.dim, lineHeight: 1.5 }}>
                <span style={{ color: C.amb, fontWeight: 600 }}>Non disponibles dans GEE :</span>{" "}
                {res.unavailable.join(" · ")}
              </div>
            )}
          </div>
        )}
      </>)}

      {/* ══ Onglet Définition ══ */}
      {tab === "info" && (
        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div>
            <div style={h}>Bassin versant</div>
            <p style={p}>
              Un bassin versant est la surface qui collecte toute l'eau s'écoulant vers un même
              point — l'<b style={{ color: C.mut }}>exutoire</b>. Il part de ce point et remonte les
              lignes de crête jusqu'aux sommets qui séparent les écoulements.
            </p>
          </div>

          <div>
            <div style={h}>Méthode</div>
            <p style={p}>
              On s'appuie sur <b style={{ color: C.mut }}>HydroSHEDS</b> (WWF), un découpage
              hydrographique mondial dérivé d'un MNT. Le sous-bassin qui contient l'exutoire est
              identifié, puis <b style={{ color: C.mut }}>tout l'amont</b> lui est agrégé en suivant
              la topologie d'écoulement (chaque sous-bassin sait dans lequel il se déverse), et le
              tout est fusionné en un seul bassin. Le réseau hydrographique provient de HydroRIVERS.
            </p>
          </div>

          <div>
            <div style={h}>Niveaux de délimitation</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
              {[
                ["Niveau 12", "sous-bassins les plus fins — délimitation précise (défaut)"],
                ["Niveau 10", "unités intermédiaires — bassins moyens à grands"],
                ["Niveau 8",  "grandes unités — contour plus grossier, calcul plus léger"],
              ].map(([lv, txt]) => (
                <div key={lv} style={{ display: "flex", gap: 8, fontSize: 10.5, lineHeight: 1.45 }}>
                  <span style={{ fontFamily: M, color: C.txt, width: 58, flexShrink: 0 }}>{lv}</span>
                  <span style={{ color: C.mut, flex: 1, minWidth: 0 }}>{txt}</span>
                </div>
              ))}
            </div>
          </div>

          <div>
            <div style={h}>Attributs calculés</div>
            <p style={p}>
              Surface et périmètre (géométrie) ; longueur du réseau et ordre de rivière
              (HydroRIVERS) ; altitude et pente (SRTM) ; sol — argile, carbone, pH (OpenLandMap) ;
              précipitation annuelle (CHIRPS) ; occupation dominante (ESA WorldCover).
            </p>
          </div>

          <div style={{ display: "flex", gap: 8, background: C.amb + "12",
                        border: `0.5px solid ${C.amb}33`, borderRadius: 7, padding: "8px 10px" }}>
            <span style={{ color: C.amb, flexShrink: 0, marginTop: 1 }}><IcAlert size={13} /></span>
            <div>
              <div style={{ fontSize: 9.5, color: C.amb, fontWeight: 600, marginBottom: 2 }}>PROXIES & LIMITES</div>
              <p style={{ ...p, fontSize: 10.5 }}>
                <b>Nappe phréatique</b> : approchée par le stockage souterrain GLDAS-CLSM (~28 km),
                pas une profondeur mesurée. <b>Permanent / temporaire</b> : déduit de l'ordre de
                rivière, non d'une observation de pérennité. <b>Unité aquifère</b> : indisponible
                (aucune couche hydrogéologique dans GEE). HydroSHEDS est à ~500 m : les très petits
                bassins sont mal résolus et la limite suit le maillage des sous-bassins.
              </p>
            </div>
          </div>

          <div>
            <div style={h}>Sources</div>
            {[
              ["HydroSHEDS / HydroBASINS / HydroRIVERS (WWF)", "https://www.hydrosheds.org/"],
              ["OpenLandMap — propriétés des sols", "https://openlandmap.org/"],
              ["CHIRPS — précipitations", "https://www.chc.ucsb.edu/data/chirps"],
            ].map(([label, href]) => (
              <a key={href} href={href} target="_blank" rel="noopener noreferrer" style={{
                display: "flex", alignItems: "center", gap: 6, marginTop: 6,
                fontFamily: F, fontSize: 10.5, color: C.acc, textDecoration: "none",
                border: `0.5px solid ${C.acc}66`, borderRadius: 6, padding: "6px 9px",
              }}>
                <IcExternalLink size={12} style={{ flexShrink: 0 }} /> {label}
              </a>
            ))}
          </div>
        </div>
      )}

      <style>{`@keyframes ws-spin{to{transform:rotate(360deg)}} .ws-spin{animation:ws-spin 1s linear infinite}`}</style>
    </div>
  );
}
