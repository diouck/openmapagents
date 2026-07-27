import { useState, useEffect, useCallback, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

function DistBar({ label, pct, color, ha }) {
  const C = useThemeContext();
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 5 }}>
      <span style={{ fontFamily: M, fontSize: 10, color: C.dim, width: 52, flexShrink: 0, textAlign: "right" }}>{label}</span>
      <div style={{ flex: 1, height: 7, background: C.hover, borderRadius: 4, overflow: "hidden" }}>
        <div style={{ width: `${Math.max(pct, 1)}%`, height: "100%", background: color, borderRadius: 4, transition: "width .5s ease" }} />
      </div>
      <span style={{ fontFamily: M, fontSize: 10, color: C.dim, width: 30, textAlign: "right", flexShrink: 0 }}>{pct.toFixed(0)}%</span>
      <span style={{ fontFamily: M, fontSize: 10, color: C.dim, width: 50, textAlign: "right", flexShrink: 0 }}>
        {ha >= 100 ? `${(ha / 100).toFixed(1)} km²` : `${ha.toFixed(0)} ha`}
      </span>
    </div>
  );
}

function StatCard({ label, value, unit, color }) {
  const C = useThemeContext();
  return (
    <div style={{ background: C.hover, borderRadius: 6, padding: "8px 10px", border: `0.5px solid ${C.bdr}`, display: "flex", flexDirection: "column", gap: 2 }}>
      <span style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>{label}</span>
      <span style={{ fontFamily: M, fontSize: 17, fontWeight: 500, color: color || C.txt, lineHeight: 1 }}>
        {value}<span style={{ fontSize: 10, fontWeight: 400, color: C.dim, marginLeft: 3 }}>{unit}</span>
      </span>
    </div>
  );
}

const CANOPY_COLORS  = ["#d9f0a3","#addd8e","#78c679","#41ab5d","#238443","#005a32"];
const CANOPY_CLASSES = [
  { label: "1–5 m",   key: "c1_5",   color: "#d9f0a3" },
  { label: "5–10 m",  key: "c5_10",  color: "#addd8e" },
  { label: "10–20 m", key: "c10_20", color: "#78c679" },
  { label: "20–30 m", key: "c20_30", color: "#41ab5d" },
  { label: "> 30 m",  key: "c30",    color: "#005a32" },
];

export default function CanopyStatsModal({ layer, bbox, roiGeoJSON, onClose }) {
  const C = useThemeContext();
  const [stats,   setStats]   = useState(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(null);

  const panelRef = useRef(null);
  const stateRef = useRef({ ox: 0, oy: 0, dragging: false });
  const [pos, setPos] = useState(null);

  useEffect(() => {
    const x = Math.max(20, window.innerWidth - 480);
    const y = 80;
    setPos({ x, y });
  }, []);

  const onDragStart = useCallback((e) => {
    if (e.button !== 0) return;
    e.preventDefault();
    const rect = panelRef.current?.getBoundingClientRect();
    stateRef.current = { ox: e.clientX - rect.left, oy: e.clientY - rect.top, dragging: true };
    const onMove = (ev) => {
      const x = Math.max(0, Math.min(window.innerWidth  - 460, ev.clientX - stateRef.current.ox));
      const y = Math.max(0, Math.min(window.innerHeight - 80,  ev.clientY - stateRef.current.oy));
      setPos({ x, y });
    };
    const onUp = () => { window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);

  useEffect(() => {
    const h = e => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [onClose]);

  const fetchStats = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const body = {};
      if (roiGeoJSON) body.roi_geojson = roiGeoJSON;
      else if (bbox)  body.bbox = bbox;
      const res  = await fetch(`${API}/api/gee/canopy/stats`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Erreur ${res.status}`);
      setStats(data);
    } catch (e) { setError(e.message); }
    setLoading(false);
  }, [bbox, roiGeoJSON]);

  useEffect(() => { fetchStats(); }, [fetchStats]);

  const exportCSV = () => {
    if (!stats) return;
    const rows = [["Métrique","Valeur","Unité"],["Hauteur moyenne",stats.mean_height,"m"],["Hauteur max",stats.max_height,"m"],["Couverture",stats.cover_pct,"%"],["Surface boisée",stats.cover_ha,"ha"],["Surface totale",stats.total_ha,"ha"],...CANOPY_CLASSES.map(c=>[`Surface ${c.label}`,stats.classes?.[c.key]?.ha??0,"ha"])];
    const csv = rows.map(r=>r.join(";")).join("\n");
    const blob = new Blob([csv],{type:"text/csv;charset=utf-8;"});
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href=url; a.download="canopy_stats.csv"; a.click();
    URL.revokeObjectURL(url);
  };

  if (!pos) return null;

  return (
    <div ref={panelRef} style={{ position:"fixed", left:pos.x, top:pos.y, width:460, maxHeight:"calc(100vh - 100px)", zIndex:500, background:C.card, border:`0.5px solid ${C.bdr}`, borderRadius:10, boxShadow:"0 4px 24px rgba(0,0,0,0.30)", display:"flex", flexDirection:"column", overflow:"hidden", userSelect:"none" }}>

      <div onMouseDown={onDragStart} style={{ padding:"10px 14px", borderBottom:`0.5px solid ${C.bdr}`, display:"flex", alignItems:"center", justifyContent:"space-between", gap:8, cursor:"grab", flexShrink:0 }}>
        <div style={{ display:"flex", alignItems:"center", gap:8 }}>
          <span style={{ fontSize:11, color:C.dim, letterSpacing:2 }}>⠿</span>
          <div>
            <div style={{ fontSize:12, fontWeight:600, color:C.txt }}>Statistiques — Hauteur de canopée</div>
            <div style={{ fontSize:9, color:C.dim, marginTop:1 }}>WRI/Meta 2020 (~1 m) · {layer?.name || "Zone affichée"}</div>
          </div>
        </div>
        <button onClick={onClose} style={{ background:"none", border:"none", cursor:"pointer", fontSize:15, color:C.dim, lineHeight:1, flexShrink:0, padding:2 }}>✕</button>
      </div>

      <div style={{ flex:1, overflowY:"auto", padding:"12px 14px", display:"flex", flexDirection:"column", gap:12 }}>

        {loading && (
          <div style={{ textAlign:"center", padding:"24px 0", color:C.dim, fontSize:12 }}>
            <div style={{ fontSize:18, marginBottom:6 }}>🛰</div>
            Calcul en cours via Google Earth Engine…
          </div>
        )}

        {error && !loading && (
          <div style={{ padding:"8px 10px", borderRadius:6, fontSize:11, background:C.red+"15", color:C.red, border:`0.5px solid ${C.red}44` }}>
            {error}
            <button onClick={fetchStats} style={{ marginLeft:8, fontSize:10, color:C.acc, background:"none", border:"none", cursor:"pointer", textDecoration:"underline" }}>Réessayer</button>
          </div>
        )}

        {stats && !loading && (<>
          <div style={{ display:"grid", gridTemplateColumns:"repeat(4, minmax(0,1fr))", gap:6 }}>
            <StatCard label="Hauteur moy." value={stats.mean_height?.toFixed(1)} unit="m" color={C.acc} />
            <StatCard label="Hauteur max." value={stats.max_height?.toFixed(0)}  unit="m" />
            <StatCard label="Couverture"   value={stats.cover_pct?.toFixed(0)}   unit="%" />
            <StatCard label="Surface" value={stats.cover_ha>=100?`${(stats.cover_ha/100).toFixed(1)}`:stats.cover_ha?.toFixed(0)} unit={stats.cover_ha>=100?"km²":"ha"} />
          </div>

          <div style={{ height:7, borderRadius:4, background:`linear-gradient(to right, ${CANOPY_COLORS.join(", ")})` }} />

          <div>
            <div style={{ fontSize:9, color:C.dim, textTransform:"uppercase", letterSpacing:".05em", marginBottom:7 }}>Distribution des hauteurs</div>
            {CANOPY_CLASSES.map(c => { const cls=stats.classes?.[c.key]||{pct:0,ha:0}; return <DistBar key={c.key} label={c.label} pct={cls.pct} ha={cls.ha} color={c.color} />; })}
          </div>

          <div>
            <div style={{ fontSize:9, color:C.dim, textTransform:"uppercase", letterSpacing:".05em", marginBottom:7 }}>Surfaces par classe</div>
            <div style={{ display:"grid", gridTemplateColumns:"repeat(2, minmax(0,1fr))", gap:5 }}>
              {CANOPY_CLASSES.map(c => {
                const cls=stats.classes?.[c.key]||{pct:0,ha:0};
                return (
                  <div key={c.key} style={{ background:C.hover, borderRadius:6, padding:"7px 9px", border:`0.5px solid ${C.bdr}`, display:"flex", alignItems:"center", gap:7 }}>
                    <div style={{ width:8, height:8, borderRadius:"50%", background:c.color, flexShrink:0 }} />
                    <div style={{ flex:1, minWidth:0 }}>
                      <div style={{ fontSize:9, color:C.dim }}>Arbres {c.label}</div>
                      <div style={{ fontFamily:M, fontSize:12, fontWeight:500, color:C.txt }}>
                        {(cls.ha/100).toFixed(2)} km²
                        <span style={{ fontFamily:F, fontSize:9, color:C.dim, fontWeight:400, marginLeft:3 }}>· {cls.ha?.toFixed(0)} ha</span>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <div style={{ fontSize:9, color:C.dim, borderTop:`0.5px solid ${C.bdr}`, paddingTop:8 }}>
            Zone : <span style={{ fontFamily:M, color:C.txt }}>{stats.total_ha?.toFixed(0)} ha</span>
            {" · "}Pixels : <span style={{ fontFamily:M, color:C.txt }}>{stats.valid_pixels?.toLocaleString()}</span>
            {" · "}Résol. : <span style={{ fontFamily:M, color:C.txt }}>{stats.scale} m</span>
          </div>
        </>)}
      </div>

      <div style={{ padding:"8px 14px", borderTop:`0.5px solid ${C.bdr}`, display:"flex", gap:6, justifyContent:"flex-end", flexShrink:0 }}>
        {stats && (
          <button onClick={exportCSV} style={{ fontFamily:F, fontSize:10, padding:"5px 10px", borderRadius:5, background:"transparent", border:`0.5px solid ${C.bdr}`, color:C.dim, cursor:"pointer" }}>↓ CSV</button>
        )}
        <button onClick={onClose} style={{ fontFamily:F, fontSize:10, fontWeight:600, padding:"5px 12px", borderRadius:5, background:C.acc, border:"none", color:"#fff", cursor:"pointer" }}>Fermer</button>
      </div>
    </div>
  );
}
