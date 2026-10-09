/**
 * DynamicalPanel.jsx — Module « Données météo ouvertes » (dynamical.org).
 *
 * Explore les jeux de données météo/climat de dynamical.org (GFS, GEFS, HRRR,
 * ECMWF, ICON-EU, IMERG, MRMS, HRDPS). Sections demandées :
 *   Dataset → Forecast run (init_time) → Lead time (échéance) → Variable [+ membre]
 *
 * Tout le calcul est serveur (cf. backend/dynamical_routes.py) : le panneau
 * demande une tranche 2D et l'ajoute en overlay image colorisé (onAddImageLayer),
 * ou interroge une série temporelle au point (clic carte) rendue en sparkline.
 *
 * Le cadre/titre est dessiné par FloatingPanel : ce composant ne dessine aucun
 * cadre. v1 validée sur noaa-gfs-forecast, généralisable (mêmes dimensions).
 */
import { useState, useEffect } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";

const API = import.meta.env.VITE_API_URL || (import.meta.env.DEV ? "http://localhost:8000" : "");

// Nom lisible d'une variable (les ids sont verbeux : temperature_2m → Température 2 m).
function prettyVar(id) {
  return id
    .replace(/_/g, " ")
    .replace(/\b2m\b/, "2 m").replace(/\b10m\b/, "10 m").replace(/\b80m\b/, "80 m").replace(/\b100m\b/, "100 m")
    .replace(/temperature/i, "température").replace(/wind u/i, "vent U").replace(/wind v/i, "vent V")
    .replace(/precipitation/i, "précipitation").replace(/relative humidity/i, "humidité rel.")
    .replace(/pressure/i, "pression").replace(/surface/i, "surface").replace(/\b\w/, c => c.toUpperCase());
}

function fmtLead(h) {
  if (h < 24) return `+${h} h`;
  const d = Math.floor(h / 24), r = h % 24;
  return r ? `+${d} j ${r} h` : `+${d} j`;
}

// init_time "2026-10-09T12Z" → "jeu. 09 oct. · 12 h UTC" (sélection de date lisible).
function fmtRun(iso) {
  try {
    const d = new Date(iso.replace("Z", ":00:00Z"));
    const date = d.toLocaleDateString("fr-FR", { weekday: "short", day: "2-digit", month: "short", timeZone: "UTC" });
    const hh = String(d.getUTCHours()).padStart(2, "0");
    return `${date} · ${hh} h UTC`;
  } catch { return iso; }
}

export default function DynamicalPanel({ mapRef, onAddImageLayer }) {
  const C = useThemeContext();

  const [cat, setCat]       = useState([]);         // catalogue
  const [dsId, setDsId]     = useState("noaa-gfs-forecast");
  const [dims, setDims]     = useState(null);       // dimensions du dataset courant
  const [variable, setVar]  = useState("temperature_2m");
  const [run, setRun]       = useState("");         // init_time (vide = dernier)
  const [leadIdx, setLeadIdx] = useState(0);        // index dans dims.lead_hours
  const [member, setMember] = useState(0);
  const [busy, setBusy]     = useState(false);
  const [err, setErr]       = useState(null);
  const [legend, setLegend] = useState(null);
  const [picking, setPicking] = useState(false);
  const [series, setSeries] = useState(null);       // { unit, series:[{lead_h,value}] }
  const [tab, setTab]       = useState("outil");    // outil | def
  const [scope, setScope]   = useState("view");     // view | world | roi
  const [roiBbox, setRoiBbox] = useState(null);     // [W,S,E,N] dessiné (2 clics)
  const [roiPicking, setRoiPicking] = useState(false);
  const [opacity, setOpacity] = useState(0.75);     // transparence de l'overlay
  // Style (option A) : palette / nb de classes / bornes min-max.
  const [palette, setPalette] = useState("auto");
  const [nClasses, setNClasses] = useState(0);      // 0 = dégradé continu
  const [vmin, setVmin] = useState("");             // vide = auto (percentiles)
  const [vmax, setVmax] = useState("");

  const mapObj = () => mapRef?.current?.getMap?.() || null;

  // Catalogue au montage.
  useEffect(() => {
    fetch(`${API}/api/dynamical/catalog`).then(r => r.json())
      .then(d => setCat(d.datasets || [])).catch(() => setErr("Catalogue indisponible."));
  }, []);

  // Dimensions à chaque changement de dataset.
  useEffect(() => {
    if (!dsId) return;
    setDims(null); setErr(null); setLegend(null); setSeries(null);
    fetch(`${API}/api/dynamical/dataset/${dsId}`).then(async r => {
      if (!r.ok) throw new Error((await r.json()).detail || r.statusText);
      return r.json();
    }).then(d => {
      setDims(d);
      setVar(v => d.variables.some(x => x.id === v) ? v : (d.variables[0]?.id || ""));
      setRun(d.init_times?.length ? d.init_times[d.init_times.length - 1] : "");
      setLeadIdx(0); setMember(0);
    }).catch(e => setErr(String(e.message || e)));
  }, [dsId]);

  const leadHours = dims?.lead_hours || [0];
  const curLead = leadHours[Math.min(leadIdx, leadHours.length - 1)] ?? 0;

  async function showField() {
    const m = mapObj();
    if (!m) { setErr("Carte non prête."); return; }
    setBusy(true); setErr(null);
    try {
      // Emprise : vue carte / monde entier (bbox null) / ROI dessinée.
      let bbox = null;
      if (scope === "view") { const b = m.getBounds(); bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]; }
      else if (scope === "roi") {
        if (!roiBbox) { setErr("Dessine d'abord la ROI (bouton ci-dessous)."); setBusy(false); return; }
        bbox = roiBbox;
      }
      const r = await fetch(`${API}/api/dynamical/field`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset: dsId, variable, init_time: run || null, lead_hours: curLead, member, bbox,
          palette: palette === "auto" ? null : palette,
          classes: nClasses || null,
          vmin: vmin === "" ? null : Number(vmin),
          vmax: vmax === "" ? null : Number(vmax),
        }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      setLegend({ items: d.legend, unit: d.unit });
      // Pré-remplit les bornes avec la plage réellement utilisée (ajustable ensuite).
      if (vmin === "" && d.vmin != null) setVmin(String(Math.round(d.vmin * 10) / 10));
      if (vmax === "" && d.vmax != null) setVmax(String(Math.round(d.vmax * 10) / 10));
      onAddImageLayer?.({
        id: `dyn_${dsId}_${variable}_${Date.now()}`,
        name: `${prettyVar(variable)} ${fmtLead(curLead)} — ${(cat.find(c => c.id === dsId) || {}).model || dsId}`,
        imageUrl: `data:image/png;base64,${d.png_b64}`,
        coordinates: d.image_coordinates, bbox: d.bbox, opacity,
        legend: d.legend, unit: d.unit,
      });
      // Recadre sur le résultat pour qu'il soit visible (sinon « rien ne s'affiche »).
      if (d.bbox) {
        const [bw, bs, be, bn] = d.bbox;
        try { m.fitBounds([[bw, bs], [be, bn]], { padding: 24, duration: 600, maxZoom: 8 }); } catch { /* noop */ }
      }
    } catch (e) { setErr(String(e.message || e)); }
    finally { setBusy(false); }
  }

  function pickPoint() {
    const m = mapObj();
    if (!m) return;
    setPicking(true); setSeries(null); setErr(null);
    m.getCanvas().style.cursor = "crosshair";
    m.once("click", async (e) => {
      m.getCanvas().style.cursor = "";
      setPicking(false); setBusy(true);
      try {
        const r = await fetch(`${API}/api/dynamical/point`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ dataset: dsId, variable, lat: e.lngLat.lat, lon: e.lngLat.lng, init_time: run || null, member }),
        });
        const d = await r.json();
        if (!r.ok) throw new Error(d.detail || r.statusText);
        setSeries(d);
      } catch (er) { setErr(String(er.message || er)); }
      finally { setBusy(false); }
    });
  }

  // ROI : deux clics sur la carte → emprise [W,S,E,N].
  function drawRoi() {
    const m = mapObj();
    if (!m) return;
    setRoiPicking(true); setErr(null);
    m.getCanvas().style.cursor = "crosshair";
    let c1 = null;
    const onClick = (e) => {
      if (!c1) { c1 = e.lngLat; return; }
      const w = Math.min(c1.lng, e.lngLat.lng), ea = Math.max(c1.lng, e.lngLat.lng);
      const s = Math.min(c1.lat, e.lngLat.lat), n = Math.max(c1.lat, e.lngLat.lat);
      setRoiBbox([w, s, ea, n]); setScope("roi");
      m.getCanvas().style.cursor = ""; setRoiPicking(false);
      m.off("click", onClick);
    };
    m.on("click", onClick);
  }

  // ── styles ─────────────────────────────────────────────────────
  const lab = { fontSize: 10, fontWeight: 600, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", margin: "0 0 4px" };
  const sel = { fontFamily: F, fontSize: 12, padding: "7px 9px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input, color: C.txt, width: "100%", outline: "none", boxSizing: "border-box" };
  const sec = { padding: "10px 12px 0" };
  const btn = (bg) => ({ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "9px 12px", borderRadius: 8, border: "none", background: bg, color: "#fff", cursor: "pointer", width: "100%" });

  // Catalogue groupé par modèle.
  const byModel = {};
  cat.forEach(c => { (byModel[c.model] ||= []).push(c); });

  const curDs = cat.find(c => c.id === dsId);
  return (
    <div style={{ display: "flex", flexDirection: "column", paddingBottom: 12 }}>
      {/* Onglets */}
      <div style={{ display: "flex", gap: 4, padding: "8px 12px 0" }}>
        {[["outil", "Réglages"], ["def", "Définition"]].map(([k, l]) => (
          <button key={k} onClick={() => setTab(k)} style={{
            flex: 1, fontFamily: F, fontSize: 11, fontWeight: 600, padding: "6px 8px", borderRadius: 6,
            cursor: "pointer", border: `0.5px solid ${C.bdr}`,
            background: tab === k ? C.acc : C.hover, color: tab === k ? "#fff" : C.txt,
          }}>{l}</button>
        ))}
      </div>

      {tab === "def" && <DefinitionTab C={C} ds={curDs} dims={dims} />}

      {tab === "outil" && <>
      {/* Dataset */}
      <div style={sec}>
        <p style={lab}>Dataset</p>
        <select style={sel} value={dsId} onChange={e => setDsId(e.target.value)}>
          {Object.entries(byModel).map(([model, items]) => (
            <optgroup key={model} label={model}>
              {items.map(c => (
                <option key={c.id} value={c.id}>
                  {c.kind === "analysis" ? "Analyse" : "Prévision"} · {c.res} · {c.domain}{c.members ? " · ensemble" : ""}
                </option>
              ))}
            </optgroup>
          ))}
        </select>
      </div>

      {!dims && !err && <p style={{ ...sec, color: C.mut, fontSize: 11 }}>Chargement des dimensions…</p>}

      {dims && (
        <>
          {/* Variable */}
          <div style={sec}>
            <p style={lab}>Variable</p>
            <select style={sel} value={variable} onChange={e => { setVar(e.target.value); setVmin(""); setVmax(""); }}>
              {dims.variables.map(v => (
                <option key={v.id} value={v.id}>{prettyVar(v.id)}{v.units ? ` (${v.units})` : ""}</option>
              ))}
            </select>
          </div>

          {/* Forecast run (init_time) — sélection de date/heure lisible */}
          {dims.init_times?.length > 0 && (
            <div style={sec}>
              <p style={lab}>Forecast run (date & heure du run)</p>
              <select style={sel} value={run} onChange={e => setRun(e.target.value)}>
                {dims.init_times.slice().reverse().map((t, i) => (
                  <option key={t} value={t}>{fmtRun(t)}{i === 0 ? " — dernier" : ""}</option>
                ))}
              </select>
            </div>
          )}

          {/* Lead time */}
          {leadHours.length > 1 && (
            <div style={sec}>
              <p style={lab}>Lead time (échéance) · <span style={{ color: C.acc }}>{fmtLead(curLead)}</span></p>
              <input type="range" min={0} max={leadHours.length - 1} step={1} value={leadIdx}
                     onChange={e => setLeadIdx(+e.target.value)} style={{ width: "100%" }} />
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9.5, color: C.mut, fontFamily: M }}>
                <span>{fmtLead(leadHours[0])}</span><span>{fmtLead(leadHours[leadHours.length - 1])}</span>
              </div>
            </div>
          )}

          {/* Membre d'ensemble */}
          {dims.members > 0 && (
            <div style={sec}>
              <p style={lab}>Membre d'ensemble (0–{dims.members - 1})</p>
              <input type="number" min={0} max={dims.members - 1} value={member}
                     onChange={e => setMember(Math.max(0, Math.min(dims.members - 1, +e.target.value)))} style={sel} />
            </div>
          )}

          {/* Emprise (périmètre) */}
          <div style={sec}>
            <p style={lab}>Emprise</p>
            <div style={{ display: "flex", gap: 4 }}>
              {[["view", "Vue carte"], ["world", "Monde"], ["roi", "ROI"]].map(([k, l]) => (
                <button key={k} onClick={() => setScope(k)} style={{
                  flex: 1, fontFamily: F, fontSize: 10.5, fontWeight: 600, padding: "6px 4px", borderRadius: 6,
                  cursor: "pointer", border: `0.5px solid ${C.bdr}`,
                  background: scope === k ? C.acc : C.hover, color: scope === k ? "#fff" : C.txt,
                }}>{l}</button>
              ))}
            </div>
            {scope === "roi" && (
              <button onClick={drawRoi} style={{
                marginTop: 6, fontFamily: F, fontSize: 10.5, padding: "6px 8px", borderRadius: 6, width: "100%",
                cursor: "pointer", border: `0.5px solid ${C.bdr}`, background: roiPicking ? C.amb : C.hover,
                color: roiPicking ? "#fff" : C.txt,
              }}>
                {roiPicking ? "2 clics : coins opposés…" : roiBbox ? "ROI définie ✓ — redessiner" : "Dessiner l'emprise (2 clics)"}
              </button>
            )}
            {scope === "world" && <p style={{ fontSize: 9.5, color: C.mut, marginTop: 4 }}>Emprise = domaine complet du dataset (peut être lourd).</p>}
          </div>

          {/* Opacité / transparence */}
          <div style={sec}>
            <p style={lab}>Opacité de la couche · <span style={{ color: C.acc }}>{Math.round(opacity * 100)} %</span></p>
            <input type="range" min={0.1} max={1} step={0.05} value={opacity}
                   onChange={e => setOpacity(+e.target.value)} style={{ width: "100%" }} />
          </div>

          {/* Style : palette / classes / bornes */}
          <div style={sec}>
            <p style={lab}>Style</p>
            <div style={{ display: "flex", gap: 6 }}>
              <select style={{ ...sel, flex: 1 }} value={palette} onChange={e => setPalette(e.target.value)} title="Palette">
                {[["auto", "Palette auto"], ["thermique", "Thermique"], ["spectral", "Spectral"], ["viridis", "Viridis"],
                  ["precip", "Précipitations"], ["vent", "Vent"], ["humidite", "Humidité"], ["gris", "Gris"]].map(([k, l]) =>
                  <option key={k} value={k}>{l}</option>)}
              </select>
              <select style={{ ...sel, width: 110 }} value={nClasses} onChange={e => setNClasses(+e.target.value)} title="Nombre de classes">
                <option value={0}>Dégradé</option>
                {[3, 4, 5, 6, 7, 8, 9, 10].map(n => <option key={n} value={n}>{n} classes</option>)}
              </select>
            </div>
            <div style={{ display: "flex", gap: 6, marginTop: 6, alignItems: "center" }}>
              <span style={{ fontSize: 10, color: C.mut }}>Min</span>
              <input type="number" value={vmin} placeholder="auto" onChange={e => setVmin(e.target.value)} style={{ ...sel, flex: 1 }} />
              <span style={{ fontSize: 10, color: C.mut }}>Max</span>
              <input type="number" value={vmax} placeholder="auto" onChange={e => setVmax(e.target.value)} style={{ ...sel, flex: 1 }} />
              <button onClick={() => { setVmin(""); setVmax(""); }} title="Réinitialiser les bornes (auto)"
                      style={{ fontFamily: F, fontSize: 10, padding: "6px 8px", borderRadius: 6, cursor: "pointer", border: `0.5px solid ${C.bdr}`, background: C.hover, color: C.txt }}>⟲</button>
            </div>
            <p style={{ fontSize: 9, color: C.mut, marginTop: 4 }}>Modifie puis clique « Afficher sur la carte » pour réappliquer le style.</p>
          </div>

          {/* Actions */}
          <div style={{ ...sec, display: "flex", flexDirection: "column", gap: 8, marginTop: 4 }}>
            <button style={btn(busy ? C.mut : C.acc)} disabled={busy} onClick={showField}>
              {busy ? "Extraction…" : "Afficher sur la carte"}
            </button>
            <button style={btn(picking ? C.amb : "#555")} disabled={busy} onClick={pickPoint}>
              {picking ? "Cliquez un point sur la carte…" : "Série temporelle (cliquer un point)"}
            </button>
          </div>

          {/* Légende du champ */}
          {legend && (
            <div style={sec}>
              <p style={lab}>Légende — {legend.unit}</p>
              <div style={{ display: "flex", height: 12, borderRadius: 3, overflow: "hidden", border: `0.5px solid ${C.bdr}` }}>
                {legend.items.map((it, i) => <div key={i} style={{ flex: 1, background: it.color }} title={`${it.value} ${legend.unit}`} />)}
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.mut, fontFamily: M, marginTop: 2 }}>
                <span>{legend.items[0]?.value}</span><span>{legend.items[legend.items.length - 1]?.value}</span>
              </div>
            </div>
          )}

          {/* Sparkline série temporelle */}
          {series?.series?.length > 1 && <Sparkline data={series} C={C} M={M} varName={prettyVar(variable)} />}
        </>
      )}

      {err && <p style={{ ...sec, color: "#e06", fontSize: 11 }}>⚠ {err}</p>}
      <p style={{ ...sec, color: C.mut, fontSize: 9.5, lineHeight: 1.5, marginTop: 6 }}>
        Source : dynamical.org (Zarr/Icechunk, calcul serveur). L'échéance s'ajuste à la valeur la plus proche disponible.
      </p>
      </>}
    </div>
  );
}

// Onglet Définition — explique la source, la structure et le dataset courant.
function DefinitionTab({ C, ds, dims }) {
  const p = { fontSize: 11.5, color: C.txt, lineHeight: 1.6, margin: "0 0 8px" };
  const h = { fontSize: 10, fontWeight: 700, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", margin: "12px 0 4px" };
  const li = { fontSize: 11, color: C.mut, lineHeight: 1.5 };
  return (
    <div style={{ padding: "12px 12px 0" }}>
      <p style={p}>
        Accès aux jeux de données météo/climat ouverts de <b>dynamical.org</b> (Zarr/Icechunk,
        <i> analysis-ready cloud-optimized</i>). Le calcul est fait côté serveur : extraction d'une
        tranche 2D (dataset · run · échéance · variable) renvoyée en overlay colorisé, ou série
        temporelle au point.
      </p>
      <p style={h}>Structure d'un dataset</p>
      <p style={li}>init_time (run du modèle) × lead_time (échéance) × latitude × longitude → variables,
        plus un membre d'ensemble pour les modèles probabilistes (GEFS, IFS-ENS, AIFS-ENS).</p>
      <p style={h}>Modèles disponibles</p>
      <p style={li}>NOAA GFS · GEFS · HRRR · MRMS · ECMWF AIFS / IFS-ENS · DWD ICON-EU · NASA IMERG · ECCC HRDPS.</p>
      {ds && (
        <>
          <p style={h}>Dataset sélectionné</p>
          <p style={li}>
            <b>{ds.model}</b> — {ds.kind === "analysis" ? "analyse" : "prévision"} · résolution {ds.res} ·
            domaine {ds.domain}{ds.members ? " · ensemble" : ""}
            {ds.lead_h ? ` · échéance jusqu'à +${ds.lead_h} h` : ""}.
            {dims ? ` ${dims.variables?.length || 0} variables, ${dims.init_times?.length || 0} runs récents` : ""}
          </p>
        </>
      )}
      <p style={h}>Utilisation</p>
      <p style={li}>Onglet <b>Réglages</b> → choisir dataset, run, échéance (slider) et variable, puis
        « Afficher sur la carte ». Le bouton « Série temporelle » trace la valeur par échéance au point cliqué.</p>
      <p style={{ ...li, marginTop: 10, color: C.mut, fontSize: 9.5 }}>Source : dynamical.org · licence des données selon le producteur (NOAA, ECMWF, DWD, NASA, ECCC).</p>
    </div>
  );
}

// Mini-graphe SVG de la série (valeur par échéance).
function Sparkline({ data, C, M, varName }) {
  const pts = data.series.filter(p => p.value != null);
  if (pts.length < 2) return null;
  const W = 230, H = 90, pad = 4;
  const xs = pts.map(p => p.lead_h), ys = pts.map(p => p.value);
  const x0 = Math.min(...xs), x1 = Math.max(...xs), y0 = Math.min(...ys), y1 = Math.max(...ys);
  const sx = v => pad + (W - 2 * pad) * (v - x0) / Math.max(1e-9, x1 - x0);
  const sy = v => H - pad - (H - 2 * pad) * (v - y0) / Math.max(1e-9, y1 - y0);
  const d = pts.map((p, i) => `${i ? "L" : "M"}${sx(p.lead_h).toFixed(1)},${sy(p.value).toFixed(1)}`).join(" ");
  return (
    <div style={{ padding: "10px 12px 0" }}>
      <p style={{ fontSize: 10, fontWeight: 600, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", margin: "0 0 4px" }}>
        {varName} — {data.unit} · {data.lat.toFixed(2)}, {data.lon.toFixed(2)}
      </p>
      <svg width="100%" viewBox={`0 0 ${W} ${H}`} style={{ background: C.input, borderRadius: 7, border: `0.5px solid ${C.bdr}` }}>
        <path d={d} fill="none" stroke={C.acc} strokeWidth="1.6" />
      </svg>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.mut, fontFamily: M, marginTop: 2 }}>
        <span>{fmtLead(x0)} · {y0.toFixed(1)}</span><span>{fmtLead(x1)} · {y1.toFixed(1)}</span>
      </div>
    </div>
  );
}
