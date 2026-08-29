/**
 * MaxarPanel.jsx — Maxar Open Data : imagerie satellite HR AVANT / APRÈS
 * catastrophe, importée directement depuis le catalogue public AWS.
 *
 * Flux : choisir un événement (inondation, incendie, cyclone, séisme…) →
 * lister ses acquisitions datées → « Ajouter » mosaïque les tuiles COG « visual »
 * de l'acquisition (POST /api/maxar/mosaic) en un overlay géoréférencé.
 * Les dates anciennes = images d'avant l'événement ; récentes = après.
 *
 * Licence des données : CC BY-NC 4.0 (non commercial), © Maxar Open Data.
 */
import { useState, useEffect, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";

export default function MaxarPanel({ mapRef, onAddImageLayer }) {
  const C = useThemeContext();
  const [tab, setTab] = useState("data");           // data | def
  const [events, setEvents] = useState([]);
  const [eid, setEid] = useState("");
  const [acq, setAcq] = useState(null);             // {title, bbox, acquisitions:[...]}
  const [inView, setInView] = useState(true);       // limiter à la vue courante
  const [quality, setQuality] = useState(4096);     // côté max du PNG (détail)
  const [busy, setBusy] = useState(false);          // chargement acquisitions
  const [adding, setAdding] = useState(null);       // catalog_id en cours de mosaïque
  const [err, setErr] = useState(null);
  const [msg, setMsg] = useState(null);

  // Liste des événements au montage.
  useEffect(() => {
    let ok = true;
    (async () => {
      try {
        const r = await fetch(`${API}/maxar/events`);
        if (!r.ok) throw new Error(`Erreur ${r.status}`);
        const d = await r.json();
        if (ok) setEvents(d.events || []);
      } catch (e) { if (ok) setErr(e.message || String(e)); }
    })();
    return () => { ok = false; };
  }, []);

  const fitTo = useCallback((bbox) => {
    if (!bbox || bbox.length !== 4) return;
    const [w, s, e, n] = bbox;
    try { mapRef?.current?.getMap?.()?.fitBounds([[w, s], [e, n]], { padding: 40, duration: 1200 }); } catch (_) {}
  }, [mapRef]);

  const loadEvent = useCallback(async (id) => {
    setEid(id); setAcq(null); setErr(null); setMsg(null);
    if (!id) return;
    setBusy(true);
    try {
      const r = await fetch(`${API}/maxar/acquisitions`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ event: id }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      setAcq(d);
      fitTo(d.bbox);
    } catch (e) { setErr(e.message || String(e)); }
    finally { setBusy(false); }
  }, [fitTo]);

  const addMosaic = useCallback(async (a) => {
    setAdding(a.catalog_id); setErr(null); setMsg(null);
    try {
      let bbox = null;
      if (inView) {
        const b = mapRef?.current?.getMap?.()?.getBounds?.();
        if (b) bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
      }
      const r = await fetch(`${API}/maxar/mosaic`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ event: eid, catalog_id: a.catalog_id, bbox, max_side: Number(quality) }),
      });
      if (!r.ok) { let m = `Erreur ${r.status}`; try { m = (await r.json()).detail || m; } catch (_) {} throw new Error(m); }
      const d = await r.json();
      onAddImageLayer?.({
        name: d.name,
        imageUrl: `data:image/png;base64,${d.png_b64}`,
        coordinates: d.image_coordinates, bbox: d.bbox, opacity: 1,
        rasterToken: null, bands: 3, fit: true,
      });
      const rez = d.ground_res_m != null
        ? ` · ~${d.ground_res_m} m/px${d.at_native ? " (résolution native)" : ""}`
        : "";
      setMsg(`Ajouté : ${d.n_tiles} tuile(s)${rez}.${d.truncated ? " Tronqué — zoomez pour affiner." : ""}${!d.at_native && !inView ? " Zoomez sur la zone avant d'ajouter pour plus de détail." : ""}`);
    } catch (e) { setErr(e.message || String(e)); }
    finally { setAdding(null); }
  }, [eid, inView, mapRef, onAddImageLayer]);

  // ── styles ──
  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 11.5, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", width: "100%", boxSizing: "border-box" };
  const Code = ({ children }) => (<code style={{ fontFamily: M, fontSize: 11.5, background: C.acc + "12", color: C.acc, padding: "1px 5px", borderRadius: 4 }}>{children}</code>);
  const tabBtn = (id, label) => (
    <button key={id} onClick={() => setTab(id)}
      style={{ fontFamily: F, fontSize: 12, fontWeight: tab === id ? 600 : 500, padding: "5px 10px", cursor: "pointer",
        background: "transparent", color: tab === id ? C.acc : C.mut, border: "none",
        borderBottom: `2px solid ${tab === id ? C.acc : "transparent"}`, marginBottom: -1 }}>
      {label}
    </button>
  );

  // Bornes de dates de l'événement (repère avant/après).
  const dates = (acq?.acquisitions || []).map((a) => a.date).filter(Boolean);
  const dRange = dates.length ? `${dates[0]} → ${dates[dates.length - 1]}` : null;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 2, borderBottom: `1px solid ${C.bdr}` }}>
        {tabBtn("data", "Catalogue")}
        {tabBtn("def", "Définition")}
      </div>

      {tab === "def" ? (
        <div style={{ flex: 1, minHeight: 0, overflow: "auto", display: "flex", flexDirection: "column", gap: 12, fontFamily: F, fontSize: 12.5, lineHeight: 1.55, color: C.txt, paddingRight: 4 }}>
          <p style={{ margin: 0 }}>Le <b>Maxar Open Data Program</b> publie gratuitement des images satellite <b>haute résolution</b> (30–50 cm) des zones frappées par des catastrophes — <b>avant et après</b> l'événement — pour l'évaluation des dégâts et les secours.</p>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Comment ça marche</div>
            <p style={{ margin: 0, color: C.mut }}>Choisissez un <b>événement</b> (inondation, incendie, cyclone, séisme, éruption…), puis une <b>acquisition</b> (une prise datée). « Ajouter » lit et <b>mosaïque</b> les tuiles Cloud-Optimized GeoTIFF (<Code>visual</Code>, RVB) de cette acquisition à la volée et les pose comme couche image géoréférencée.</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Avant / après</div>
            <p style={{ margin: 0, color: C.mut }}>Les acquisitions sont triées par <b>date</b> : les plus anciennes montrent la zone <b>avant</b> la catastrophe, les plus récentes <b>après</b>. Ajoutez-en deux et comparez-les (Comparateur A/B, opacité, ou couches empilées).</p>
          </div>
          <div>
            <div style={{ fontWeight: 600, marginBottom: 3 }}>Emprise</div>
            <p style={{ margin: 0, color: C.mut }}>Une acquisition peut couvrir une large bande. « <b>Limiter à la vue</b> » ne mosaïque que les tuiles visibles à l'écran (plus rapide, plus net) ; décochez pour toute l'acquisition (bornée à 40 tuiles). Les zones sans tuile restent transparentes.</p>
          </div>
          <div style={{ background: C.bg2 || C.bg, border: `1px solid ${C.bdr}`, borderRadius: 8, padding: "8px 10px", color: C.mut, fontSize: 11.5 }}>
            <b>Licence : CC BY-NC 4.0</b> — usage <b>non commercial</b>, attribution « © Maxar Open Data ». Données lues en direct depuis le bucket AWS public <Code>maxar-opendata</Code> (aucun compte requis). Sources : <Code>maxar.com/open-data</Code>, <Code>registry.opendata.aws/maxar-open-data</Code>.
          </div>
        </div>
      ) : (
        <>
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut, lineHeight: 1.5 }}>
            Imagerie HR <b>avant/après</b> catastrophe (Maxar Open Data). Choisissez un événement puis ajoutez une acquisition datée à la carte.
          </div>

          <div>
            <div style={lbl}>Événement · catastrophe</div>
            <select value={eid} onChange={(e) => loadEvent(e.target.value)} style={inp}>
              <option value="">— {events.length ? `${events.length} événements` : "chargement…"} —</option>
              {events.map((e) => <option key={e.id} value={e.id}>{e.title}</option>)}
            </select>
          </div>

          <label style={{ display: "flex", alignItems: "center", gap: 7, fontFamily: F, fontSize: 11.5, color: C.txt, cursor: "pointer" }}>
            <input type="checkbox" checked={inView} onChange={(e) => setInView(e.target.checked)} />
            Limiter à la vue courante <span style={{ color: C.dim }}>(plus net)</span>
          </label>

          <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
            <span style={{ ...lbl, marginBottom: 0 }}>Détail</span>
            <select value={quality} onChange={(e) => setQuality(Number(e.target.value))} style={{ ...inp, width: "auto", flex: 1 }}>
              <option value={4096}>Haute déf. (4096 px)</option>
              <option value={2048}>Standard (2048 px)</option>
              <option value={1024}>Léger (1024 px)</option>
            </select>
          </div>
          <div style={{ fontFamily: F, fontSize: 10, color: C.dim, marginTop: -3 }}>
            L'aperçu est une image figée (pas des tuiles) : plus vous <b>zoomez sur la zone avant d'ajouter</b>, plus c'est net — jusqu'à la résolution native (~0,5 m).
          </div>

          {err && <div style={{ fontFamily: M, fontSize: 11.5, color: "#e11d1d", background: "#e11d1d14", border: "0.5px solid #e11d1d55", borderRadius: 6, padding: "6px 10px", whiteSpace: "pre-wrap" }}>{err}</div>}
          {msg && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{msg}</div>}

          {busy && <div style={{ fontFamily: F, fontSize: 11.5, color: C.mut }}>Chargement des acquisitions…</div>}

          {acq && (
            <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", gap: 6 }}>
              <div style={{ fontFamily: F, fontSize: 11, color: C.mut }}>
                {acq.count} acquisition{acq.count > 1 ? "s" : ""} · {acq.title}
                {dRange && <span style={{ color: C.dim }}> · {dRange}</span>}
              </div>
              <div style={{ flex: 1, minHeight: 0, overflowY: "auto", display: "flex", flexDirection: "column", gap: 6 }}>
                {acq.acquisitions.map((a) => (
                  <div key={a.catalog_id} style={{ display: "flex", alignItems: "center", gap: 8, padding: "7px 9px", border: `0.5px solid ${C.bdr}`, borderRadius: 8, background: C.bg2 || C.bg }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontFamily: F, fontSize: 12, fontWeight: 600, color: C.txt }}>{a.date || "date ?"}</div>
                      <div style={{ fontFamily: M, fontSize: 10, color: C.dim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={a.catalog_id}>
                        {a.n_tiles} tuile{a.n_tiles > 1 ? "s" : ""} · {a.catalog_id}
                      </div>
                    </div>
                    <button onClick={() => addMosaic(a)} disabled={adding === a.catalog_id || !a.n_tiles}
                      style={{ fontFamily: F, fontSize: 11, fontWeight: 500, padding: "5px 11px", flexShrink: 0,
                        cursor: adding === a.catalog_id ? "wait" : (a.n_tiles ? "pointer" : "not-allowed"),
                        background: adding === a.catalog_id ? C.acc : "transparent",
                        color: adding === a.catalog_id ? "#fff" : C.acc,
                        border: `1px solid ${C.acc}66`, borderRadius: 6, opacity: a.n_tiles ? 1 : 0.4 }}>
                      {adding === a.catalog_id ? "Mosaïque…" : "Ajouter"}
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}

          {acq && (
            <div style={{ fontFamily: F, fontSize: 10, color: C.dim, borderTop: `0.5px solid ${C.bdr}`, paddingTop: 6 }}>
              Licence <b>CC BY-NC 4.0</b> · © Maxar Open Data (usage non commercial).
            </div>
          )}
        </>
      )}
    </div>
  );
}
