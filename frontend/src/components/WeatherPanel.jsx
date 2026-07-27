/**
 * WeatherPanel.jsx — Module « Météo & temps réel ».
 *
 * Onglets « Temps réel » (couches + FRISE dans le panneau + légende + palettes) et
 * « Définition ». Frise UNIFIÉE passé→futur :
 *   • Radar de précipitation → RainViewer (client, sans clé) ; palette au choix
 *     (schéma de couleurs RainViewer 0-8 : bleu classique … orage éclatant).
 *   • Satellite IR → NASA GIBS GOES-Est (RainViewer ne sert plus l'IR) ; régional.
 *   • Prévision GFS (POST /api/gee/weather/gfs) → temp / pluie / vent, +48 h ;
 *     précipitations en palette radar avec transparence hors pluie.
 *
 * Bascule de trame en FONDU ENCHAÎNÉ (rasterAnim `fade`) = anti-clignotement.
 */
import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import {
  IcRadar, IcCloud, IcCloudRain, IcThermometer, IcDroplets, IcWind,
  IcPlay, IcPause, IcInfo, IcAlert,
} from "../icons";
import { showFrame, setFrameOpacity, clearAnim } from "../utils/rasterAnim";
import maplibregl from "maplibre-gl";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";
const RV_JSON = "https://api.rainviewer.com/public/weather-maps.json";
const BID = { radar: "wx_radar", ir: "wx_ir", gfs: "wx_gfs" };
const GFS_VARS = [
  ["temp",   "Température 2 m", IcThermometer],
  ["precip", "Précipitations",  IcDroplets],
  ["wind",   "Vent 10 m",       IcWind],
];
// Schémas de couleurs radar RainViewer + un dégradé pour la légende.
const RADAR_PALS = [
  ["Bleu",   2, "linear-gradient(90deg,#e6f2ff,#99ccff,#3399ff,#0066cc,#002b66)"],
  ["Orage",  7, "linear-gradient(90deg,#00c8ff,#00e400,#c6ff00,#ffd400,#ff9000,#ff2600,#d000c0)"],
  ["Météo",  4, "linear-gradient(90deg,#00ccff,#00cc00,#ffff00,#ff9900,#ff0000)"],
  ["Sombre", 8, "linear-gradient(90deg,#141428,#16536b,#1f9e89,#a6d96a,#ffd400,#ff5a00)"],
];
const IR_GRAD = "linear-gradient(90deg,#111111,#555555,#999999,#dddddd,#ffffff)";

// IR satellite via NASA GIBS — géostationnaires RÉGIONAUX (le mondial libre en
// tuiles n'existe pas). On choisit le satellite selon la zone regardée.
const IR_SATS = {
  "goes-east": { label: "GOES-Est",   layer: "GOES-East_ABI_Band13_Clean_Infrared", lon: -75 },
  "goes-west": { label: "GOES-Ouest", layer: "GOES-West_ABI_Band13_Clean_Infrared", lon: -137 },
  "himawari":  { label: "Himawari",   layer: "Himawari_AHI_Band13_Clean_Infrared",  lon: 140 },
};
function pickIrLayer(irSat, lon) {
  if (irSat !== "auto" && IR_SATS[irSat]) return IR_SATS[irSat].layer;
  let best = "goes-east", bd = 999;
  for (const k in IR_SATS) {
    let d = Math.abs(lon - IR_SATS[k].lon) % 360; if (d > 180) d = 360 - d;
    if (d < bd) { bd = d; best = k; }
  }
  return IR_SATS[best].layer;
}
// Instants alignés sur 10 min (latence ~25 min), sur ~2 h, pour le layer choisi.
function buildGibsIr(layer) {
  const step = 10 * 60 * 1000, latency = 25 * 60 * 1000;
  const end = Math.floor((Date.now() - latency) / step) * step;
  const out = [];
  for (let t = end - 2 * 3600 * 1000; t <= end; t += step) {
    const iso = new Date(t).toISOString().replace(/\.\d+Z$/, "Z");
    out.push({ time: Math.floor(t / 1000),
      url: `https://gibs.earthdata.nasa.gov/wmts/epsg3857/best/${layer}/default/${iso}/GoogleMapsCompatible_Level6/{z}/{y}/{x}.png` });
  }
  return out;
}

const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
const idxNearest = (arr, t) => {
  let best = 0, bd = Infinity;
  for (let i = 0; i < arr.length; i++) { const d = Math.abs(arr[i] - t); if (d < bd) { bd = d; best = i; } }
  return best;
};
const nearestFrame = (frames, T) => {
  if (!frames || !frames.length) return null;
  let r = frames[0];
  for (const f of frames) { if (f.time <= T) r = f; else break; }
  return r;
};
const fmt = (u) => {
  if (u == null) return "—";
  const d = new Date(u * 1000), now = new Date();
  const same = d.toDateString() === now.toDateString();
  const hh = String(d.getHours()).padStart(2, "0"), mm = String(d.getMinutes()).padStart(2, "0");
  const day = same ? "auj." : d.toLocaleDateString("fr", { day: "2-digit", month: "2-digit" });
  return `${day} ${hh}:${mm}`;
};

export default function WeatherPanel({ mapRef }) {
  const C = useThemeContext();

  const [tab, setTab]       = useState("direct");
  const [on, setOn]         = useState({ radar: true, ir: false, gfs: false });
  const [gfsVar, setGfsVar] = useState("precip");
  const [precipPal, setPrecipPal] = useState("storm");
  const [radarColor, setRadarColor] = useState(7);
  const [irSat, setIrSat] = useState("auto");
  const [mapLon, setMapLon] = useState(0);
  const [opacity, setOpacity] = useState(0.9);
  const [live, setLive]     = useState(true);

  const [rvData, setRvData] = useState({ host: "", radar: [] });
  const [gfsFrames, setGfsFrames] = useState([]);
  const [gfsMeta, setGfsMeta] = useState(null);
  const [gfsBusy, setGfsBusy] = useState(false);
  const [rvAt, setRvAt]     = useState(null);
  const [err, setErr]       = useState(null);

  const [curT, setCurT]     = useState(null);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed]   = useState(1);
  const [loop, setLoop]     = useState(true);

  const trackRef = useRef(null);
  const popupRef = useRef(null);
  const insRef = useRef({});
  const map = () => mapRef?.current?.getMap?.() || null;

  // Longitude du centre de carte → choix auto du satellite IR régional.
  useEffect(() => {
    const m = map(); if (!m) return;
    const upd = () => { try { setMapLon(Math.round(m.getCenter().lng)); } catch (_) {} };
    upd();
    m.on("moveend", upd);
    return () => { try { m.off("moveend", upd); } catch (_) {} };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── RainViewer (radar) ──
  const loadRV = useCallback(async () => {
    try {
      const r = await fetch(RV_JSON);
      const j = await r.json();
      const host = j.host || "https://tilecache.rainviewer.com";
      const radar = [...(j.radar?.past || []), ...(j.radar?.nowcast || [])]
        .map(f => ({ time: f.time, path: f.path }));
      setRvData({ host, radar });
      setRvAt(Date.now());
      setErr(null);
    } catch (_) { setErr("Flux RainViewer indisponible (radar)."); }
  }, []);

  useEffect(() => { loadRV(); }, [loadRV]);
  useEffect(() => {
    if (!live) return;
    const id = setInterval(loadRV, 5 * 60 * 1000);
    return () => clearInterval(id);
  }, [live, loadRV]);

  // ── Prévision GFS ──
  const loadGfs = useCallback(async (variable, palette) => {
    setGfsBusy(true);
    try {
      const r = await fetch(`${API}/api/gee/weather/gfs`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ variable, palette }),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail || `Erreur ${r.status}`);
      setGfsFrames((d.steps || []).map(s => ({ time: s.valid_unix, url: s.tile_url, hours: s.hours })));
      setGfsMeta({ legend: d.legend, unit: d.unit, run_iso: d.run_iso, vis: d.vis_params });
      setErr(null);
    } catch (e) { setErr(`Prévision GFS : ${e.message}`); setGfsFrames([]); }
    setGfsBusy(false);
  }, []);

  useEffect(() => {
    if (on.gfs) loadGfs(gfsVar, precipPal);
    else setGfsFrames([]);
  }, [on.gfs, gfsVar, precipPal, loadGfs]);

  // ── Trames dérivées (radar : URL selon la palette ; IR : GIBS régional) ──
  const irLayer = useMemo(() => pickIrLayer(irSat, mapLon), [irSat, mapLon]);
  const irFrames = useMemo(() => buildGibsIr(irLayer), [irLayer, rvAt]);
  const frames = useMemo(() => ({
    radar: rvData.radar.map(f => ({ time: f.time, url: `${rvData.host}${f.path}/256/{z}/{x}/{y}/${radarColor}/1_1.png` })),
    ir: irFrames,
    gfs: gfsFrames,
  }), [rvData, radarColor, irFrames, gfsFrames]);

  // ── Frise maîtresse ──
  const masterTimes = useMemo(() => {
    const s = new Set();
    if (on.radar) frames.radar.forEach(f => s.add(f.time));
    if (on.ir)    frames.ir.forEach(f => s.add(f.time));
    if (on.gfs)   frames.gfs.forEach(f => s.add(f.time));
    return [...s].sort((a, b) => a - b);
  }, [on, frames]);

  const tMin = masterTimes[0];
  const tMax = masterTimes[masterTimes.length - 1];
  const span = (tMax != null && tMin != null) ? Math.max(1, tMax - tMin) : 1;
  const nowU = Date.now() / 1000;

  useEffect(() => {
    if (!masterTimes.length) return;
    setCurT(prev => prev == null ? clamp(nowU, tMin, tMax) : clamp(prev, tMin, tMax));
  }, [masterTimes]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Lecture ──
  useEffect(() => {
    if (tab !== "direct" || !playing || masterTimes.length < 2) return;
    const id = setInterval(() => {
      setCurT(prev => {
        const ni = idxNearest(masterTimes, prev) + 1;
        if (ni >= masterTimes.length) { if (loop) return masterTimes[0]; setPlaying(false); return prev; }
        return masterTimes[ni];
      });
    }, 700 / speed);
    return () => clearInterval(id);
  }, [tab, playing, speed, loop, masterTimes]);

  // ── Rendu carte (fondu enchaîné) ──
  useEffect(() => {
    const m = map(); if (!m) return;
    if (tab !== "direct" || curT == null) {
      [BID.ir, BID.gfs, BID.radar].forEach(b => clearAnim(m, b));
      return;
    }
    const fade = Math.round(Math.min(450, (700 / speed) * 0.75));
    const apply = (enabled, baseId, list) => {
      if (enabled && list.length) {
        const fr = nearestFrame(list, curT);
        if (fr) showFrame(m, baseId, fr.url, opacity, fade);
      } else {
        clearAnim(m, baseId);
      }
    };
    apply(on.ir,    BID.ir,    frames.ir);
    apply(on.gfs,   BID.gfs,   frames.gfs);
    apply(on.radar, BID.radar, frames.radar);
  }, [curT, on, frames, tab]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    const m = map(); if (!m) return;
    [BID.radar, BID.ir, BID.gfs].forEach(b => setFrameOpacity(m, b, opacity));
  }, [opacity]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => () => {
    const m = map(); if (!m) return;
    [BID.radar, BID.ir, BID.gfs].forEach(b => clearAnim(m, b));
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // « Touchez pour inspecter » : valeur du modèle GFS au point cliqué (popup carte).
  useEffect(() => {
    const m = map(); if (!m || tab !== "direct" || !on.gfs) return;
    const onClick = async (e) => {
      const { lng, lat } = e.lngLat;
      const s = insRef.current;
      const fr = nearestFrame(s.gfs, s.curT);
      const hours = (fr && fr.hours != null) ? fr.hours : 0;
      const vlabel = (GFS_VARS.find(v => v[0] === s.gfsVar) || [])[1] || s.gfsVar;
      if (!popupRef.current) popupRef.current = new maplibregl.Popup({ closeButton: true, closeOnClick: false, maxWidth: "240px" });
      const pop = popupRef.current;
      pop.setLngLat([lng, lat]).setHTML(`<div style="font:12px system-ui;color:#111">Lecture…</div>`).addTo(m);
      try {
        const r = await fetch(`${API}/api/gee/weather/point`, {
          method: "POST", headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ lon: lng, lat, variable: s.gfsVar, hours }),
        });
        const d = await r.json();
        if (!r.ok) throw new Error(d.detail || "err");
        const v = d.value == null ? "hors couverture" : `${d.value} ${d.unit || ""}`;
        pop.setHTML(`<div style="font:12px/1.5 system-ui;color:#111"><b>${vlabel}</b> · +${hours} h<br><span style="font-size:17px;font-weight:600">${v}</span><br><span style="color:#777;font-size:10px">${lat.toFixed(2)}, ${lng.toFixed(2)}</span></div>`);
      } catch (_) {
        pop.setHTML(`<div style="font:12px system-ui;color:#b00">Valeur indisponible</div>`);
      }
    };
    m.on("click", onClick);
    return () => { try { m.off("click", onClick); popupRef.current && popupRef.current.remove(); } catch (_) {} };
  }, [tab, on.gfs]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Interactions frise ──
  const onTrackDown = (e) => {
    const el = trackRef.current; if (!el || !masterTimes.length) return;
    const rect = el.getBoundingClientRect();
    setPlaying(false);
    const move = (ev) => {
      const cx = ev.clientX ?? (ev.touches && ev.touches[0]?.clientX);
      if (cx == null) return;
      setCurT(clamp(tMin + ((cx - rect.left) / rect.width) * span, tMin, tMax));
    };
    move(e);
    const up = () => { window.removeEventListener("pointermove", move); window.removeEventListener("pointerup", up); };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };

  const regime = curT != null && curT > nowU ? "prévu (GFS)" : "observé";
  const pct = (t) => clamp(((t - tMin) / span), 0, 1) * 100;
  const hasTL = masterTimes.length > 0 && curT != null;
  const radarGrad = (RADAR_PALS.find(p => p[1] === radarColor) || RADAR_PALS[1])[2];
  const irActiveLabel = (Object.values(IR_SATS).find(s => s.layer === irLayer) || {}).label || "GOES-Est";
  insRef.current = { curT, gfsVar, gfs: frames.gfs };

  // ── Styles ──
  const box = { background: C.bg, border: `0.5px solid ${C.bdr}`, borderRadius: 9, padding: 10, display: "flex", flexDirection: "column", gap: 9 };
  const ph  = { fontSize: 9, letterSpacing: ".08em", textTransform: "uppercase", color: C.dim, margin: "0 0 2px" };
  const hh  = { fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".06em", marginBottom: 5 };
  const pp  = { fontSize: 11.5, color: C.mut, lineHeight: 1.65, margin: 0 };
  const sw  = (active) => ({ marginLeft: "auto", width: 32, height: 18, borderRadius: 20, position: "relative", flexShrink: 0, cursor: "pointer",
    background: active ? C.acc : C.bdr, transition: "background .2s" });
  const knob = (active) => ({ position: "absolute", top: 2, left: active ? 16 : 2, width: 14, height: 14, borderRadius: "50%", background: "#fff", transition: "left .2s" });
  const chip = (active) => ({ fontFamily: F, fontSize: 10.5, padding: "4px 9px", borderRadius: 6, cursor: "pointer",
    border: `0.5px solid ${active ? C.acc : C.bdr}`, background: active ? C.acc + "1c" : "transparent", color: active ? C.acc : C.mut });

  const Layer = ({ k, label, sub, Icon, color }) => (
    <div style={{ display: "flex", alignItems: "center", gap: 9 }}>
      <span style={{ width: 26, height: 26, borderRadius: 7, border: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", justifyContent: "center", color, flexShrink: 0 }}><Icon size={14} /></span>
      <span>
        <span style={{ display: "block", fontSize: 12, fontWeight: 500, color: C.txt, lineHeight: 1.1 }}>{label}</span>
        <span style={{ display: "block", fontSize: 9.5, color: C.dim }}>{sub}</span>
      </span>
      <span style={sw(on[k])} onClick={() => setOn(o => ({ ...o, [k]: !o[k] }))}><span style={knob(on[k])} /></span>
    </div>
  );

  const Bar = ({ grad, ticks, label }) => (
    <div>
      <div style={{ fontSize: 9.5, color: C.mut, marginBottom: 4 }}>{label}</div>
      <div style={{ height: 9, borderRadius: 4, background: grad, border: `0.5px solid ${C.bdr}` }} />
      <div style={{ display: "flex", justifyContent: "space-between", fontFamily: M, fontSize: 8.5, color: C.dim, marginTop: 2 }}>
        {ticks.map((t, i) => <span key={i}>{t}</span>)}
      </div>
    </div>
  );

  const anyLegend = on.radar || on.ir || (on.gfs && gfsMeta?.vis);

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      {/* Onglets */}
      <div style={{ display: "flex", gap: 4, borderBottom: `0.5px solid ${C.bdr}`, padding: "0 12px", flexShrink: 0 }}>
        {[["direct", "Temps réel"], ["info", "Définition"]].map(([k, l]) => (
          <button key={k} onClick={() => setTab(k)} style={{
            flex: 1, fontFamily: F, fontSize: 11.5, fontWeight: tab === k ? 600 : 400, padding: "8px 2px",
            cursor: "pointer", background: "transparent", border: "none",
            borderBottom: `2px solid ${tab === k ? C.acc : "transparent"}`, color: tab === k ? C.acc : C.dim,
            marginBottom: -0.5, display: "flex", alignItems: "center", justifyContent: "center", gap: 4,
          }}>{k === "info" && <IcInfo size={12} />}{l}</button>
        ))}
      </div>

      {/* ── DÉFINITION ── */}
      {tab === "info" && (
        <div style={{ padding: 14, display: "flex", flexDirection: "column", gap: 14, overflowY: "auto" }}>
          <div>
            <div style={hh}>Météo temps réel</div>
            <p style={pp}>Deux logiques sur une <b style={{ color: C.mut }}>même frise</b> : <b style={{ color: C.mut }}>observer</b>
              (radar, nuages) et <b style={{ color: C.mut }}>prévoir</b> (GFS). Le curseur déroule le temps ; chaque couche
              affiche sa trame la plus proche.</p>
          </div>
          <div>
            <div style={hh}>Les couches</div>
            <p style={pp}><b style={{ color: C.mut }}>Radar (RainViewer)</b> : réflectivité des précipitations, pas de 10 min,
              passé ~2 h + nowcast. Palette au choix (bleu classique … orage éclatant).</p>
            <p style={{ ...pp, marginTop: 5 }}><b style={{ color: C.mut }}>Satellite IR</b> : NASA GIBS (GOES-Est) — nuages en
              infrarouge, ~10 min. Couverture Amériques / Atlantique (le mondial libre n'existe pas en tuiles).</p>
            <p style={{ ...pp, marginTop: 5 }}><b style={{ color: C.mut }}>Prévision (GFS)</b> : NOAA via Earth Engine —
              température, précipitations (palette radar, transparente hors pluie) ou vent, jusqu'à +48 h.</p>
          </div>
          <div style={{ display: "flex", gap: 8, background: C.amb + "12", border: `0.5px solid ${C.amb}33`, borderRadius: 7, padding: "8px 10px" }}>
            <span style={{ color: C.amb, flexShrink: 0, marginTop: 1 }}><IcAlert size={13} /></span>
            <div>
              <div style={{ fontSize: 9.5, color: C.amb, fontWeight: 600, marginBottom: 2 }}>À SAVOIR</div>
              <p style={pp}>Bascule de trame en fondu enchaîné (anti-clignotement). L'IR GOES-Est est régional : hors de sa
                zone, la couche reste vide. Changer de fond de carte peut masquer les couches — relancez une lecture.</p>
            </div>
          </div>
        </div>
      )}

      {/* ── TEMPS RÉEL ── */}
      {tab === "direct" && (
        <div style={{ padding: 12, display: "flex", flexDirection: "column", gap: 12, overflowY: "auto" }}>
          <div style={{ fontSize: 10.5, color: C.dim, lineHeight: 1.5 }}>
            Radar, nuages et prévision sur une <b style={{ color: C.mut }}>même frise</b> : à gauche l'observé, à droite le
            prévu. Le lecteur ci-dessous déroule le temps.
          </div>

          {/* Couches */}
          <div>
            <div style={ph}>Couches</div>
            <div style={box}>
              <Layer k="radar" label="Radar de précipitation" sub="RainViewer · pas 10 min" Icon={IcRadar} color={C.red} />
              <Layer k="ir" label="Satellite IR (nuages)" sub={`GIBS ${irActiveLabel} · régional`} Icon={IcCloud} color={C.mut} />
              <Layer k="gfs" label="Prévision (GFS)" sub={gfsBusy ? "chargement…" : "jusqu'à +72 h"} Icon={IcCloudRain} color={C.amb} />
              {on.radar && (
                <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap", borderTop: `0.5px solid ${C.bdr}`, paddingTop: 8 }}>
                  <span style={{ fontSize: 9.5, color: C.dim, width: "100%" }}>Palette radar</span>
                  {RADAR_PALS.map(([lbl, code]) => (
                    <button key={code} onClick={() => setRadarColor(code)} style={chip(radarColor === code)}>{lbl}</button>
                  ))}
                </div>
              )}
              {on.ir && (
                <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap", borderTop: `0.5px solid ${C.bdr}`, paddingTop: 8 }}>
                  <span style={{ fontSize: 9.5, color: C.dim, width: "100%" }}>Satellite IR (selon la zone)</span>
                  {[["Auto", "auto"], ["GOES-Est", "goes-east"], ["GOES-Ouest", "goes-west"], ["Himawari", "himawari"]].map(([lbl, k]) => (
                    <button key={k} onClick={() => setIrSat(k)} style={chip(irSat === k)}>{lbl}</button>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Frise temporelle */}
          <div>
            <div style={ph}>Défilement temporel</div>
            <div style={box}>
              {hasTL ? (<>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <button onClick={() => setPlaying(p => !p)} style={{ width: 32, height: 32, borderRadius: "50%", background: C.acc, color: "#fff", border: "none", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0 }}>
                    {playing ? <IcPause size={14} /> : <IcPlay size={14} />}
                  </button>
                  <span style={{ fontFamily: M, fontSize: 12, color: C.txt }}>{fmt(curT)}</span>
                  <span style={{ fontSize: 9.5, color: curT > nowU ? C.amb : C.acc, marginLeft: "auto" }}>{regime}</span>
                </div>
                <div ref={trackRef} onPointerDown={onTrackDown} style={{ position: "relative", height: 26, cursor: "pointer", touchAction: "none" }}>
                  <div style={{ position: "absolute", top: 10, left: 0, right: 0, height: 6, borderRadius: 3, overflow: "hidden", display: "flex" }}>
                    <div style={{ width: `${pct(nowU)}%`, background: `linear-gradient(90deg, ${C.blu}66, ${C.acc}88)` }} />
                    <div style={{ flex: 1, background: `repeating-linear-gradient(45deg, ${C.amb}55 0 5px, transparent 5px 10px)` }} />
                  </div>
                  <div style={{ position: "absolute", top: 5, left: `${pct(nowU)}%`, width: 2, height: 16, background: C.txt, borderRadius: 1 }} />
                  <div style={{ position: "absolute", top: 5, left: `${pct(curT)}%`, transform: "translateX(-50%)", width: 15, height: 15, borderRadius: "50%", background: "#fff", border: `3px solid ${C.acc}`, boxShadow: "0 1px 4px rgba(0,0,0,0.4)" }} />
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", fontFamily: M, fontSize: 8.5, color: C.dim }}>
                  <span>{fmt(tMin)}</span><span>maintenant</span><span>{fmt(tMax)}</span>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 4, borderTop: `0.5px solid ${C.bdr}`, paddingTop: 8 }}>
                  {[0.5, 1, 2, 4].map(s => (
                    <button key={s} onClick={() => setSpeed(s)} style={{ fontFamily: M, fontSize: 10, padding: "3px 8px", borderRadius: 5, cursor: "pointer",
                      border: `0.5px solid ${speed === s ? C.acc : C.bdr}`, background: speed === s ? C.acc : "transparent", color: speed === s ? "#fff" : C.dim }}>{s}×</button>
                  ))}
                  <button onClick={() => setLoop(v => !v)} title="Boucle" style={{ marginLeft: "auto", fontFamily: M, fontSize: 10, padding: "3px 9px", borderRadius: 5, cursor: "pointer",
                    border: `0.5px solid ${loop ? C.acc : C.bdr}`, background: loop ? C.acc + "22" : "transparent", color: loop ? C.acc : C.dim }}>⟲ Boucle</button>
                </div>
              </>) : (
                <div style={{ fontSize: 10.5, color: C.dim, textAlign: "center", padding: "6px 0" }}>Chargement des trames…</div>
              )}
            </div>
          </div>

          {/* Variable GFS + palette précip */}
          {on.gfs && (
            <div>
              <div style={ph}>Variable de prévision</div>
              <div style={box}>
                {GFS_VARS.map(([k, label, Icon]) => (
                  <div key={k} onClick={() => setGfsVar(k)} style={{ display: "flex", alignItems: "center", gap: 9, padding: "5px 4px", borderRadius: 6, cursor: "pointer",
                    background: gfsVar === k ? C.hover : "transparent", color: gfsVar === k ? C.txt : C.mut }}>
                    <Icon size={14} style={{ color: gfsVar === k ? C.acc : C.dim }} />
                    <span style={{ fontSize: 11.5 }}>{label}</span>
                    {gfsVar === k && <span style={{ marginLeft: "auto", width: 7, height: 7, borderRadius: "50%", background: C.acc }} />}
                  </div>
                ))}
                {gfsVar === "precip" && (
                  <div style={{ display: "flex", alignItems: "center", gap: 6, borderTop: `0.5px solid ${C.bdr}`, paddingTop: 8 }}>
                    <span style={{ fontSize: 9.5, color: C.dim }}>Palette</span>
                    <button onClick={() => setPrecipPal("storm")} style={chip(precipPal === "storm")}>Orage</button>
                    <button onClick={() => setPrecipPal("blue")} style={chip(precipPal === "blue")}>Bleu</button>
                  </div>
                )}
                {gfsMeta?.run_iso && <div style={{ fontSize: 9.5, color: C.dim }}>Cycle GFS {gfsMeta.run_iso} UTC</div>}
                <div style={{ display: "flex", gap: 6, fontSize: 9.5, color: C.acc, borderTop: `0.5px solid ${C.bdr}`, paddingTop: 7 }}>
                  <IcInfo size={11} style={{ flexShrink: 0, marginTop: 1 }} /><span>Touchez la carte pour lire la valeur au point.</span>
                </div>
              </div>
            </div>
          )}

          {/* Affichage */}
          <div>
            <div style={ph}>Affichage</div>
            <div style={box}>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <span style={{ fontSize: 10.5, color: C.dim }}>Opacité</span>
                <input type="range" min="0" max="100" value={Math.round(opacity * 100)} onChange={e => setOpacity(parseInt(e.target.value) / 100)} style={{ flex: 1, accentColor: C.acc }} />
                <span style={{ fontFamily: M, fontSize: 10, color: C.txt, width: 34, textAlign: "right" }}>{Math.round(opacity * 100)}%</span>
              </div>
              <div style={{ display: "flex", alignItems: "center", gap: 9 }}>
                <span style={sw(live)} onClick={() => setLive(v => !v)}><span style={knob(live)} /></span>
                <span style={{ fontSize: 11, color: C.mut }}>Mode direct (rafraîchi ~5 min)</span>
              </div>
              {rvAt && <div style={{ fontSize: 9.5, color: C.dim }}>Dernières trames à {new Date(rvAt).toLocaleTimeString("fr", { hour: "2-digit", minute: "2-digit" })}</div>}
            </div>
          </div>

          {/* Légende */}
          {anyLegend && (
            <div>
              <div style={ph}>Légende</div>
              <div style={{ ...box, gap: 11 }}>
                {on.radar && <Bar grad={radarGrad} ticks={["faible", "modéré", "fort", "intense"]} label="Radar — intensité de pluie" />}
                {on.ir && <Bar grad={IR_GRAD} ticks={["chaud", "froid (nuages hauts)"]} label="Satellite IR — sommets de nuages" />}
                {on.gfs && gfsMeta?.vis && (
                  <Bar grad={`linear-gradient(90deg, ${(gfsMeta.vis.palette || []).join(",")})`}
                    ticks={[`${gfsMeta.vis.min}`, `${Math.round((gfsMeta.vis.min + gfsMeta.vis.max) / 2)}`, `${gfsMeta.vis.max} ${gfsMeta.unit || ""}`]}
                    label={gfsMeta.legend || "Prévision GFS"} />
                )}
              </div>
            </div>
          )}

          {err && (
            <div style={{ display: "flex", gap: 6, fontSize: 10.5, color: C.red, background: C.red + "12", borderRadius: 6, padding: "7px 9px" }}>
              <IcAlert size={12} style={{ flexShrink: 0, marginTop: 1 }} /> {err}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
