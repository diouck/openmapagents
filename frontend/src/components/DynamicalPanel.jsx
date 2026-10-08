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
      const b = m.getBounds();
      const bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
      const r = await fetch(`${API}/api/dynamical/field`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dataset: dsId, variable, init_time: run || null, lead_hours: curLead, member, bbox }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || r.statusText);
      setLegend({ items: d.legend, unit: d.unit });
      onAddImageLayer?.({
        id: `dyn_${dsId}_${variable}_${Date.now()}`,
        name: `${prettyVar(variable)} ${fmtLead(curLead)} — ${(cat.find(c => c.id === dsId) || {}).model || dsId}`,
        imageUrl: `data:image/png;base64,${d.png_b64}`,
        coordinates: d.image_coordinates, bbox: d.bbox, opacity: 0.8,
        legend: d.legend, unit: d.unit,
      });
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

  // ── styles ─────────────────────────────────────────────────────
  const lab = { fontSize: 10, fontWeight: 600, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em", margin: "0 0 4px" };
  const sel = { fontFamily: F, fontSize: 12, padding: "7px 9px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input, color: C.txt, width: "100%", outline: "none", boxSizing: "border-box" };
  const sec = { padding: "10px 12px 0" };
  const btn = (bg) => ({ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "9px 12px", borderRadius: 8, border: "none", background: bg, color: "#fff", cursor: "pointer", width: "100%" });

  // Catalogue groupé par modèle.
  const byModel = {};
  cat.forEach(c => { (byModel[c.model] ||= []).push(c); });

  return (
    <div style={{ display: "flex", flexDirection: "column", paddingBottom: 12 }}>
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
            <select style={sel} value={variable} onChange={e => setVar(e.target.value)}>
              {dims.variables.map(v => (
                <option key={v.id} value={v.id}>{prettyVar(v.id)}{v.units ? ` (${v.units})` : ""}</option>
              ))}
            </select>
          </div>

          {/* Forecast run (init_time) */}
          {dims.init_times?.length > 0 && (
            <div style={sec}>
              <p style={lab}>Forecast run (init)</p>
              <select style={sel} value={run} onChange={e => setRun(e.target.value)}>
                {dims.init_times.slice().reverse().map(t => <option key={t} value={t}>{t}</option>)}
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
