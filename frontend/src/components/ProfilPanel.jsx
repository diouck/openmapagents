/**
 * ProfilPanel.jsx v3 — Profil Altimétrique
 * ✓ Persistance localStorage (+ sync API si connecté)
 * ✓ Max 10 profils sauvegardés avec vignettes SVG
 * ✓ Toutes les données conservées : points, bbox, stats, styles, options
 * ✓ UI améliorée : dashboard cards, sauvegarde après calcul
 */

import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import { useThemeContext } from "../theme";
import { F, M, API } from "../config";
import { IcCheck, IcX, IcMountain, IcStack, IcMapPin, IcEdit, IcLeaf, IcBuilding,
  IcAlert, IcSave, IcImage, IcFile, IcFolder } from "../icons";

const MAX_PROFILS = 10;
const STORAGE_KEY = "openmap_profils_v3";

// ── Couleurs pente ─────────────────────────────────────────────
const PENTE_SEUILS = [
  { max: 3,   color: "#1D9E75", label: "≤ 3% — PMR OK",         id: "facile"    },
  { max: 5,   color: "#EF9F27", label: "3–5% — vélo OK",        id: "moyen"     },
  { max: 8,   color: "#D85A30", label: "5–8% — difficile",      id: "difficile" },
  { max: 999, color: "#E24B4A", label: "> 8% — très difficile", id: "extreme"   },
];
function penteColor(pct) {
  for (const s of PENTE_SEUILS) if (Math.abs(pct) <= s.max) return s.color;
  return PENTE_SEUILS[PENTE_SEUILS.length - 1].color;
}

// ── Hook persistance localStorage + API sync ──────────────────
function useProfilStorage() {
  const load = () => {
    try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]"); }
    catch { return []; }
  };
  const [profils, setProfils] = useState(load);

  const persist = useCallback((list) => {
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(list)); } catch {}
    const token = localStorage.getItem("auth_token");
    if (token) {
      fetch(`${API}/api/profils`, {
        method: "PUT",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({ profils: list }),
      }).catch(() => {});
    }
  }, []);

  const addProfil = useCallback((profil) => {
    setProfils(prev => {
      const next = [profil, ...prev].slice(0, MAX_PROFILS);
      persist(next);
      return next;
    });
  }, [persist]);

  const deleteProfil = useCallback((id) => {
    setProfils(prev => {
      const next = prev.filter(p => p.id !== id);
      persist(next);
      return next;
    });
    const token = localStorage.getItem("auth_token");
    if (token) {
      fetch(`${API}/api/profils/${id}`, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${token}` },
      }).catch(() => {});
    }
  }, [persist]);

  // Charger depuis API au montage si connecté (overwrite localStorage si plus récent)
  useEffect(() => {
    const token = localStorage.getItem("auth_token");
    if (!token) return;
    fetch(`${API}/api/profils`, { headers: { Authorization: `Bearer ${token}` } })
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data?.profils?.length) {
          setProfils(data.profils);
          try { localStorage.setItem(STORAGE_KEY, JSON.stringify(data.profils)); } catch {}
        }
      })
      .catch(() => {});
  }, []);

  return { profils, addProfil, deleteProfil };
}

// ── Miniature SVG statique ─────────────────────────────────────
function ProfilThumb({ points, showCanopy, showBuildings }) {
  if (!points?.length) return null;
  const W = 200, H = 56, PAD = { top: 4, right: 3, bottom: 6, left: 3 };
  const gW = W - PAD.left - PAD.right;
  const gH = H - PAD.top - PAD.bottom;
  const alts = points.map(p => p.alt);
  const maxD = points[points.length - 1].dist || 1;
  const minA = Math.min(...alts);
  const maxABase   = Math.max(...alts);
  const maxACanopy = showCanopy    ? Math.max(...points.map(p => p.alt + (p.canopy   || 0))) : maxABase;
  const maxABuild  = showBuildings ? Math.max(...points.map(p => p.alt + (p.building || 0))) : maxABase;
  const maxA  = Math.max(maxABase, maxACanopy, maxABuild);
  const range = (maxA - minA) * 1.1 || 1;
  const tx = d => PAD.left + (d / maxD) * gW;
  const ty = a => PAD.top  + gH - ((a - minA) / range) * gH;
  const pathD = points.map((p,i) => `${i===0?"M":"L"}${tx(p.dist).toFixed(1)},${ty(p.alt).toFixed(1)}`).join(" ");
  const fillD = `${pathD} L${tx(maxD)},${(PAD.top+gH).toFixed(1)} L${PAD.left},${(PAD.top+gH).toFixed(1)} Z`;
  const segs  = points.slice(1).map((p,i) => ({ x1:tx(points[i].dist),y1:ty(points[i].alt),x2:tx(p.dist),y2:ty(p.alt),c:penteColor(p.slope) }));

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width:"100%", height:"100%", display:"block" }}>
      <defs>
        <linearGradient id="tg" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#1D9E75" stopOpacity="0.28"/>
          <stop offset="100%" stopColor="#1D9E75" stopOpacity="0.03"/>
        </linearGradient>
      </defs>
      {showCanopy && points.some(p=>p.canopy>0) && (() => {
        const ft = points.map((p,i)=>`${i===0?"M":"L"}${tx(p.dist).toFixed(1)},${ty(p.alt+(p.canopy||0)).toFixed(1)}`).join(" ");
        const fb = [...points].reverse().map(p=>`L${tx(p.dist).toFixed(1)},${ty(p.alt).toFixed(1)}`).join(" ");
        return <path d={`${ft} ${fb} Z`} fill="#4CAF5030"/>;
      })()}
      {showBuildings && points.filter(p=>p.building>0).map((p,i)=>{
        const top=ty(p.alt+p.building), bot=ty(p.alt);
        return <rect key={i} x={tx(p.dist)-1} y={top} width={2} height={Math.max(1,bot-top)} fill="#88888855"/>;
      })}
      <path d={fillD} fill="url(#tg)"/>
      {segs.map((s,i)=><line key={i} x1={s.x1} y1={s.y1} x2={s.x2} y2={s.y2} stroke={s.c} strokeWidth="1.4" strokeLinecap="round"/>)}
    </svg>
  );
}

// ── Card vignette ──────────────────────────────────────────────
function ProfilCard({ profil, onLoad, onDelete, C }) {
  const [confirmDel, setConfirmDel] = useState(false);
  const st = profil.stats;
  const distStr = st?.dist < 1 ? `${(st.dist*1000).toFixed(0)} m` : `${st?.dist?.toFixed(1)} km`;
  const dateStr = profil.savedAt
    ? new Date(profil.savedAt).toLocaleDateString("fr-FR", { day:"2-digit", month:"short" })
    : "";

  return (
    <div style={{ borderRadius:9, border:`0.5px solid ${C.bdr}`, background:C.card, overflow:"hidden", display:"flex", flexDirection:"column" }}>
      {/* Vignette cliquable */}
      <div onClick={()=>onLoad(profil)} title="Charger ce profil"
        style={{ height:60, background:C.hover, cursor:"pointer", padding:"3px 5px 0", overflow:"hidden", flexShrink:0 }}>
        <ProfilThumb points={profil.points} showCanopy={profil.options?.showCanopy} showBuildings={profil.options?.showBuildings}/>
      </div>
      {/* Corps card */}
      <div style={{ padding:"7px 9px 8px", flex:1, display:"flex", flexDirection:"column", gap:4 }}>
        <div style={{ display:"flex", alignItems:"flex-start", justifyContent:"space-between", gap:4 }}>
          <div style={{ flex:1, minWidth:0 }}>
            <div style={{ fontSize:10, fontWeight:600, color:C.txt, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis" }}>
              {profil.title || "Sans titre"}
            </div>
            {profil.desc && (
              <div style={{ fontSize:8, color:C.dim, marginTop:1, whiteSpace:"nowrap", overflow:"hidden", textOverflow:"ellipsis" }}>
                {profil.desc}
              </div>
            )}
          </div>
          {confirmDel ? (
            <div style={{ display:"flex", gap:2, flexShrink:0 }}>
              <button onClick={()=>{onDelete(profil.id);setConfirmDel(false);}} style={{ padding:"2px 5px", borderRadius:3, background:"#E24B4A18", border:"0.5px solid #E24B4A55", color:"#E24B4A", cursor:"pointer", display:"flex" }}><IcCheck size={11}/></button>
              <button onClick={()=>setConfirmDel(false)} style={{ padding:"2px 5px", borderRadius:3, background:"transparent", border:`0.5px solid ${C.bdr}`, color:C.dim, cursor:"pointer", display:"flex" }}><IcX size={11}/></button>
            </div>
          ) : (
            <button onClick={()=>setConfirmDel(true)} title="Supprimer"
              style={{ fontFamily:F, fontSize:10, padding:"0 4px", borderRadius:3, background:"transparent", border:"none", color:C.dim, cursor:"pointer", lineHeight:1.5, flexShrink:0 }}>·· </button>
          )}
        </div>

        {/* Chips stats */}
        <div style={{ display:"flex", gap:4, flexWrap:"wrap" }}>
          {st?.dist != null && <span style={{ fontSize:8, color:C.acc, fontFamily:M, background:C.acc+"12", padding:"1px 4px", borderRadius:3 }}>{distStr}</span>}
          {st?.dPlus != null && <span style={{ fontSize:8, color:"#1D9E75", background:"#1D9E7512", padding:"1px 4px", borderRadius:3, fontFamily:M }}>↑{st.dPlus}m</span>}
          {st?.altMax != null && <span style={{ fontSize:8, color:C.amb, background:C.amb+"12", padding:"1px 4px", borderRadius:3, fontFamily:M }}>{st.altMax}m</span>}
          <span style={{ fontSize:8, color:C.dim, padding:"1px 3px" }}>{profil.source==="IGN"?"IGN":"SRTM"}</span>
        </div>

        {/* Pied */}
        <div style={{ display:"flex", alignItems:"center", justifyContent:"space-between", marginTop:2 }}>
          <span style={{ fontSize:8, color:C.dim }}>{dateStr}</span>
          <button onClick={()=>onLoad(profil)} style={{ fontFamily:F, fontSize:9, padding:"2px 8px", borderRadius:4, background:C.acc+"15", border:`0.5px solid ${C.acc}44`, color:C.acc, cursor:"pointer", fontWeight:600 }}>
            ↩ Charger
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Séparateur ────────────────────────────────────────────────
function Sep({ children }) {
  const C = useThemeContext();
  return (
    <div style={{ fontSize:9, color:C.dim, textTransform:"uppercase", letterSpacing:".06em", fontWeight:600, borderBottom:`0.5px solid ${C.bdr}`, paddingBottom:4, marginTop:2 }}>
      {children}
    </div>
  );
}

// ── Stat box ──────────────────────────────────────────────────
function StatBox({ label, value, color, unit }) {
  const C = useThemeContext();
  return (
    <div style={{ flex:1, background:C.hover, borderRadius:6, padding:"6px 8px", border:`0.5px solid ${C.bdr}`, display:"flex", flexDirection:"column", gap:2, minWidth:0 }}>
      <div style={{ fontSize:9, color:C.dim, textTransform:"uppercase", letterSpacing:".04em" }}>{label}</div>
      <div style={{ fontSize:13, fontWeight:700, color:color||C.txt, fontFamily:M, lineHeight:1 }}>
        {value}<span style={{ fontSize:9, fontWeight:400, color:C.dim, marginLeft:2 }}>{unit}</span>
      </div>
    </div>
  );
}

// ── Graphique SVG interactif ───────────────────────────────────
function ProfilSVG({ points, hoverIdx, onHover, crossLayers, showCanopy, showBuildings, id="profil-svg-main" }) {
  const C = useThemeContext();
  const svgRef = useRef(null);
  const W = 480, H = 160, PAD = { top:12, right:12, bottom:24, left:40 };
  const gW = W - PAD.left - PAD.right;
  const gH = H - PAD.top - PAD.bottom;
  if (!points.length) return null;

  const alts = points.map(p=>p.alt);
  const maxD = points[points.length-1].dist || 1;
  const minA = Math.min(...alts);
  const maxABase   = Math.max(...alts);
  const maxACanopy = showCanopy    ? Math.max(...points.map(p=>p.alt+(p.canopy||0)))   : maxABase;
  const maxABuild  = showBuildings ? Math.max(...points.map(p=>p.alt+(p.building||0))) : maxABase;
  const maxA   = Math.max(maxABase, maxACanopy, maxABuild);
  const rangeA = (maxA - minA) * 1.05 || 1;

  const toX = d => PAD.left + (d/maxD)*gW;
  const toY = a => PAD.top  + gH - ((a-minA)/rangeA)*gH;

  const pathD = points.map((p,i)=>`${i===0?"M":"L"}${toX(p.dist).toFixed(1)},${toY(p.alt).toFixed(1)}`).join(" ");
  const fillD = `${pathD} L${toX(maxD)},${(PAD.top+gH).toFixed(1)} L${PAD.left},${(PAD.top+gH).toFixed(1)} Z`;
  const segs  = points.slice(1).map((p,i)=>({ x1:toX(points[i].dist),y1:toY(points[i].alt),x2:toX(p.dist),y2:toY(p.alt),c:penteColor(p.slope) }));
  const peaks = points.filter(p=>p.isPeak||p.isValley);
  const hPt   = hoverIdx!=null ? points[hoverIdx] : null;

  const handleMouseMove = useCallback((e) => {
    if (!svgRef.current) return;
    const rect = svgRef.current.getBoundingClientRect();
    const dataX = (((e.clientX-rect.left)/rect.width)*W - PAD.left) / gW * maxD;
    if (dataX < 0 || dataX > maxD) { onHover(null); return; }
    let closest=0, minDiff=Infinity;
    points.forEach((p,i)=>{ const d=Math.abs(p.dist-dataX); if(d<minDiff){minDiff=d;closest=i;} });
    onHover(closest);
  }, [points, maxD, onHover]);

  return (
    <svg id={id} ref={svgRef} width="100%" viewBox={`0 0 ${W} ${H}`}
      style={{ display:"block", cursor:"crosshair" }}
      onMouseMove={handleMouseMove} onMouseLeave={()=>onHover(null)}>
      <defs>
        <linearGradient id={`fg-${id}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={C.acc} stopOpacity="0.35"/>
          <stop offset="100%" stopColor={C.acc} stopOpacity="0.04"/>
        </linearGradient>
        <linearGradient id={`cg-${id}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor="#4CAF50" stopOpacity="0.55"/>
          <stop offset="100%" stopColor="#4CAF50" stopOpacity="0.15"/>
        </linearGradient>
        <clipPath id={`cp-${id}`}><rect x={PAD.left} y={PAD.top} width={gW} height={gH}/></clipPath>
      </defs>

      {/* Grille */}
      {Array.from({length:5},(_,i)=>{
        const y=PAD.top+(i/4)*gH;
        return <g key={i}>
          <line x1={PAD.left} y1={y} x2={PAD.left+gW} y2={y} stroke={C.bdr} strokeWidth="0.5"/>
          <text x={PAD.left-3} y={y+3} textAnchor="end" fill={C.dim} fontSize="7" fontFamily={M}>{Math.round(maxA-(i/4)*rangeA)}</text>
        </g>;
      })}
      {Array.from({length:6},(_,i)=>{
        const x=PAD.left+(i/5)*gW, val=(i/5)*maxD;
        return <g key={i}>
          <line x1={x} y1={PAD.top} x2={x} y2={PAD.top+gH} stroke={C.bdr} strokeWidth="0.5"/>
          <text x={x} y={H-4} textAnchor="middle" fill={C.dim} fontSize="7" fontFamily={M}>{val<1?`${(val*1000).toFixed(0)}m`:`${val.toFixed(1)}km`}</text>
        </g>;
      })}

      {/* Croisements */}
      {crossLayers?.map((cl,ci)=>cl.segments?.map((seg,si)=>(
        <rect key={`${ci}-${si}`} x={toX(seg.start)} y={PAD.top} width={Math.max(1,toX(seg.end)-toX(seg.start))} height={gH} fill={cl.color} opacity="0.18" clipPath={`url(#cp-${id})`}/>
      )))}

      {/* Remplissage terrain */}
      <path d={fillD} fill={`url(#fg-${id})`} clipPath={`url(#cp-${id})`}/>

      {/* Canopée empilée */}
      {showCanopy && points.some(p=>p.canopy>0) && (()=>{
        const ft=points.map((p,i)=>`${i===0?"M":"L"}${toX(p.dist).toFixed(1)},${toY(p.alt+(p.canopy||0)).toFixed(1)}`).join(" ");
        const fb=[...points].reverse().map(p=>`L${toX(p.dist).toFixed(1)},${toY(p.alt).toFixed(1)}`).join(" ");
        return <g clipPath={`url(#cp-${id})`}>
          <path d={`${ft} ${fb} Z`} fill={`url(#cg-${id})`}/>
          <path d={ft} fill="none" stroke="#4CAF50" strokeWidth="1.5" opacity="0.9"/>
        </g>;
      })()}

      {/* Bâti empilé */}
      {showBuildings && points.some(p=>p.building>0) && (
        <g clipPath={`url(#cp-${id})`}>
          {points.filter(p=>p.building>0).map((p,i)=>{
            const top=toY(p.alt+p.building), bot=toY(p.alt);
            return <rect key={i} x={toX(p.dist)-1.5} y={top} width={3} height={Math.max(1,bot-top)} fill="#888" opacity="0.65"/>;
          })}
        </g>
      )}

      {/* Segments pente */}
      <g clipPath={`url(#cp-${id})`}>
        {segs.map((s,i)=><line key={i} x1={s.x1} y1={s.y1} x2={s.x2} y2={s.y2} stroke={s.c} strokeWidth="2.5" strokeLinecap="round"/>)}
      </g>
      <path d={pathD} fill="none" stroke={C.acc} strokeWidth="1" opacity="0.4" clipPath={`url(#cp-${id})`}/>

      {/* Pics/creux */}
      {peaks.map((p,i)=>(
        <g key={i}>
          <circle cx={toX(p.dist)} cy={toY(p.alt)} r="3" fill={p.isPeak?C.amb:C.blu} stroke={C.card} strokeWidth="1"/>
          <text x={toX(p.dist)} y={toY(p.alt)+(p.isPeak?-5:10)} textAnchor="middle" fill={p.isPeak?C.amb:C.blu} fontSize="7" fontFamily={M} fontWeight="600">{Math.round(p.alt)}m</text>
        </g>
      ))}

      {/* Axes */}
      <line x1={PAD.left} y1={PAD.top} x2={PAD.left} y2={PAD.top+gH} stroke={C.dim} strokeWidth="0.5"/>
      <line x1={PAD.left} y1={PAD.top+gH} x2={PAD.left+gW} y2={PAD.top+gH} stroke={C.dim} strokeWidth="0.5"/>
      <text x={7} y={PAD.top+gH/2} textAnchor="middle" fill={C.dim} fontSize="7" fontFamily={M} transform={`rotate(-90,7,${PAD.top+gH/2})`}>Alt (m)</text>

      {/* Curseur hover */}
      {hPt && (()=>{
        const tx=toX(hPt.dist), ty=toY(hPt.alt), flip=tx>W*0.65, bx=flip?tx-70:tx+5;
        return <g>
          <line x1={tx} y1={PAD.top} x2={tx} y2={PAD.top+gH} stroke={C.txt} strokeWidth="1" strokeDasharray="3,2" opacity="0.5"/>
          <circle cx={tx} cy={ty} r="4" fill={C.card} stroke={penteColor(hPt.slope)} strokeWidth="2"/>
          <rect x={bx} y={ty-18} width={65} height={34} rx="3" fill={C.card} stroke={C.bdr} strokeWidth="0.5" opacity="0.95"/>
          <text x={bx+3} y={ty-7} fill={C.txt} fontSize="8" fontFamily={M} fontWeight="600">{Math.round(hPt.alt)} m</text>
          <text x={bx+3} y={ty+3} fill={C.dim} fontSize="7" fontFamily={M}>{hPt.dist<1?`${(hPt.dist*1000).toFixed(0)} m`:`${hPt.dist.toFixed(2)} km`}</text>
          <text x={bx+3} y={ty+12} fill={penteColor(hPt.slope)} fontSize="7" fontFamily={M}>{hPt.slope>0?"+":""}{hPt.slope.toFixed(1)}%</text>
        </g>;
      })()}
    </svg>
  );
}

// ════════════════════════════════════════════════════════════════
export default function ProfilPanel({ mapRef, layers=[], drawPoints, onClearDraw, onDrawModeChange, onSetProfilLayer }) {
  const C = useThemeContext();
  const { profils, addProfil, deleteProfil } = useProfilStorage();

  const [view,          setView]          = useState("calc");     // "calc" | "saved"
  const [mode,          setMode]          = useState("polyline");
  const [layerId,       setLayerId]       = useState("");
  const [loading,       setLoading]       = useState(false);
  const [error,         setError]         = useState(null);
  const [points,        setPoints]        = useState([]);
  const [hoverIdx,      setHoverIdx]      = useState(null);
  const [source,        setSource]        = useState(null);
  const [crossIds,      setCrossIds]      = useState([]);
  const [crossData,     setCrossData]     = useState([]);
  const [showCanopy,    setShowCanopy]    = useState(false);
  const [showBuildings, setShowBuildings] = useState(false);
  const [hasCanopy,     setHasCanopy]     = useState(false);
  const [hasBuildings,  setHasBuildings]  = useState(false);
  const [expTitle,      setExpTitle]      = useState("");
  const [expDesc,       setExpDesc]       = useState("");
  const [saveMsg,       setSaveMsg]       = useState(null);
  const [bbox,          setBbox]          = useState(null);

  const stats = useMemo(() => {
    if (!points.length) return null;
    const alts  = points.map(p=>p.alt);
    const dPlus  = points.slice(1).reduce((s,p,i)=>{ const d=p.alt-points[i].alt; return s+(d>0?d:0); }, 0);
    const dMinus = points.slice(1).reduce((s,p,i)=>{ const d=p.alt-points[i].alt; return s+(d<0?-d:0); }, 0);
    const slopes = points.map(p=>p.slope);
    return {
      dist:     points[points.length-1].dist,
      altMin:   Math.min(...alts),
      altMax:   Math.max(...alts),
      altMoy:   Math.round(alts.reduce((s,v)=>s+v,0)/alts.length),
      dPlus:    Math.round(dPlus),
      dMinus:   Math.round(dMinus),
      slopeMax: Math.max(...slopes.map(Math.abs)).toFixed(1),
      slopeMoy: (slopes.reduce((s,v)=>s+Math.abs(v),0)/slopes.length).toFixed(1),
    };
  }, [points]);

  useEffect(() => {
    onDrawModeChange?.(mode==="polyline"?"polyline":mode==="twopoints"?"twopoints":null);
  }, [mode, onDrawModeChange]);

  // Curseur carte sync
  useEffect(() => {
    if (!mapRef?.current || hoverIdx==null || !points[hoverIdx]) return;
    const pt=points[hoverIdx], map=mapRef.current.getMap?.();
    if (!map) return;
    const gj={ type:"FeatureCollection", features:[{ type:"Feature", geometry:{ type:"Point", coordinates:[pt.lng,pt.lat] }, properties:{ alt:pt.alt } }] };
    try {
      if (map.getSource("profil-cursor")) map.getSource("profil-cursor").setData(gj);
      else {
        map.addSource("profil-cursor",{ type:"geojson", data:gj });
        map.addLayer({ id:"profil-cursor-layer", type:"circle", source:"profil-cursor", paint:{ "circle-radius":8,"circle-color":"#ffffff","circle-stroke-width":3,"circle-stroke-color":"#1D9E75","circle-opacity":0.9 } });
      }
    } catch {}
  }, [hoverIdx, points, mapRef]);

  useEffect(() => {
    if (hoverIdx!=null) return;
    const map=mapRef?.current?.getMap?.();
    if (!map) return;
    try {
      if (map.getLayer("profil-cursor-layer")) map.removeLayer("profil-cursor-layer");
      if (map.getSource("profil-cursor"))      map.removeSource("profil-cursor");
    } catch {}
  }, [hoverIdx, mapRef]);

  const lineLayers = layers.filter(l=>!l.isRaster&&l.geojson?.features?.some(f=>f.geometry?.type==="LineString"||f.geometry?.type==="MultiLineString"));
  const polyLayers = layers.filter(l=>!l.isRaster&&l.geojson?.features?.some(f=>f.geometry?.type==="Polygon"||f.geometry?.type==="MultiPolygon"));

  // ── Calcul ────────────────────────────────────────────────────
  const compute = useCallback(async () => {
    setLoading(true); setError(null); setPoints([]); setCrossData([]); setBbox(null);
    let coords = [];
    if (mode==="polyline"||mode==="twopoints") {
      if (!drawPoints?.length||drawPoints.length<2) {
        setError(mode==="twopoints"?"Posez 2 points sur la carte.":"Tracez au moins 2 points (double-clic pour terminer).");
        setLoading(false); return;
      }
      coords = mode==="twopoints"&&drawPoints.length>2 ? [drawPoints[0],drawPoints[drawPoints.length-1]] : drawPoints;
    } else {
      const layer=layers.find(l=>l.id===layerId);
      if (!layer) { setError("Sélectionnez une couche ligne"); setLoading(false); return; }
      const feat=layer.geojson.features.find(f=>f.geometry?.type==="LineString"||f.geometry?.type==="MultiLineString");
      if (!feat) { setError("Aucune géométrie LineString trouvée"); setLoading(false); return; }
      coords=feat.geometry.type==="LineString"?feat.geometry.coordinates:feat.geometry.coordinates.flat();
    }
    if (coords.length) {
      const lons=coords.map(c=>c[0]), lats=coords.map(c=>c[1]);
      setBbox([Math.min(...lons),Math.min(...lats),Math.max(...lons),Math.max(...lats)]);
    }
    const cld=crossIds.map(id=>{ const l=layers.find(x=>x.id===id); return l?{id,name:l.name,color:l.color,features:l.geojson.features}:null; }).filter(Boolean);
    try {
      const res=await fetch(`${API}/elevation/profile`,{ method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({ coordinates:coords, cross_layers:cld, with_canopy:showCanopy, with_buildings:showBuildings }) });
      if (!res.ok) { const e=await res.json().catch(()=>({})); throw new Error(e.detail||`Erreur ${res.status}`); }
      const data=await res.json();
      setPoints(data.points); setSource(data.source); setCrossData(data.cross_segments||[]);
      setHasCanopy(data.has_canopy||false); setHasBuildings(data.has_buildings||false);
      if (data.slope_geojson) onSetProfilLayer?.(data.slope_geojson);
    } catch(e) { setError(e.message); }
    setLoading(false);
  }, [mode, drawPoints, layerId, layers, crossIds, showCanopy, showBuildings, onSetProfilLayer]);

  // ── Sauvegarde ────────────────────────────────────────────────
  const saveProfil = useCallback(() => {
    if (!points.length||!stats) return;
    if (profils.length>=MAX_PROFILS) {
      setSaveMsg({ type:"warn", text:`Limite ${MAX_PROFILS} profils atteinte — supprimez-en un d'abord.` });
      setTimeout(()=>setSaveMsg(null), 3500); return;
    }
    addProfil({
      id:       `p_${Date.now()}`,
      savedAt:  new Date().toISOString(),
      title:    expTitle || `Profil ${new Date().toLocaleDateString("fr-FR")}`,
      desc:     expDesc,
      source, bbox, stats, points, crossData,
      options:  { showCanopy, hasCanopy, showBuildings, hasBuildings, mode },
      crossLayers: crossIds.map(id=>{ const l=layers.find(x=>x.id===id); return l?{id,name:l.name,color:l.color}:null; }).filter(Boolean),
    });
    setSaveMsg({ type:"ok", text:"Profil sauvegardé localement" });
    setTimeout(()=>setSaveMsg(null), 2500);
  }, [points, stats, expTitle, expDesc, source, bbox, crossData, crossIds, showCanopy, hasCanopy, showBuildings, hasBuildings, mode, layers, profils.length, addProfil]);

  // ── Charger un profil ─────────────────────────────────────────
  const loadProfil = useCallback((profil) => {
    setPoints(profil.points||[]); setSource(profil.source||null);
    setCrossData(profil.crossData||[]);
    setHasCanopy(profil.options?.hasCanopy||false); setHasBuildings(profil.options?.hasBuildings||false);
    setShowCanopy(profil.options?.showCanopy||false); setShowBuildings(profil.options?.showBuildings||false);
    setExpTitle(profil.title||""); setExpDesc(profil.desc||""); setBbox(profil.bbox||null);
    setView("calc");
    if (profil.bbox&&mapRef?.current) {
      try {
        const [w,s,e,n]=profil.bbox;
        mapRef.current.getMap?.()?.fitBounds([[w,s],[e,n]],{ padding:60, duration:800 });
      } catch {}
    }
  }, [mapRef]);

  // ── Export PNG ────────────────────────────────────────────────
  const exportPNG = useCallback(() => {
    const svgEl=document.getElementById("profil-svg-main");
    if (!svgEl) return;
    const svgW=960, svgH=260, headerH=(expTitle||expDesc)?52:0, footerH=28, totalH=headerH+svgH+footerH;
    const canvas=document.createElement("canvas"); canvas.width=svgW; canvas.height=totalH;
    const ctx=canvas.getContext("2d");
    ctx.fillStyle="#ffffff"; ctx.fillRect(0,0,svgW,totalH);
    if (expTitle) { ctx.font="bold 18px 'DM Sans',sans-serif"; ctx.fillStyle="#1a1a18"; ctx.fillText(expTitle,16,26); }
    if (expDesc)  { ctx.font="12px 'DM Sans',sans-serif"; ctx.fillStyle="#6b6b63"; ctx.fillText(expDesc,16,headerH-8); }
    const svgData=new XMLSerializer().serializeToString(svgEl);
    const img=new Image();
    img.onload=()=>{
      ctx.drawImage(img,0,headerH,svgW,svgH);
      ctx.strokeStyle="#e0e0e0"; ctx.lineWidth=0.5;
      ctx.beginPath(); ctx.moveTo(0,headerH+svgH+2); ctx.lineTo(svgW,headerH+svgH+2); ctx.stroke();
      ctx.font="9px 'DM Sans',sans-serif"; ctx.textAlign="left";
      let lx=16; const fy=headerH+svgH+18;
      [{color:"#1D9E75",label:"≤3% PMR"},{color:"#EF9F27",label:"3–5% vélo"},{color:"#D85A30",label:"5–8% diff."},{color:"#E24B4A",label:">8% ext."}]
        .forEach(({color,label})=>{ ctx.strokeStyle=color; ctx.lineWidth=3; ctx.beginPath(); ctx.moveTo(lx,fy-3); ctx.lineTo(lx+16,fy-3); ctx.stroke(); lx+=20; ctx.fillStyle="#6b6b63"; ctx.fillText(label,lx,fy); lx+=ctx.measureText(label).width+12; });
      if (showCanopy&&hasCanopy) { ctx.fillStyle="#4CAF5044"; ctx.fillRect(lx,fy-9,13,9); ctx.strokeStyle="#4CAF50"; ctx.lineWidth=1.5; ctx.strokeRect(lx,fy-9,13,9); lx+=17; ctx.fillStyle="#6b6b63"; ctx.fillText("Canopée",lx,fy); lx+=ctx.measureText("Canopée").width+12; }
      if (showBuildings&&hasBuildings) { ctx.fillStyle="#88888899"; ctx.fillRect(lx+3,fy-9,7,9); lx+=15; ctx.fillStyle="#6b6b63"; ctx.fillText("Bâti",lx,fy); }
      ctx.font="9px 'DM Sans',sans-serif"; ctx.fillStyle="#9a9a90"; ctx.textAlign="right";
      ctx.fillText(`OpenMapAgents · ${source==="IGN"?"IGN RGE Alti 1m":"SRTM GEE 30m"} · ${new Date().toLocaleDateString("fr-FR",{year:"numeric",month:"long",day:"numeric"})}`, svgW-16, fy);
      const a=document.createElement("a"); a.href=canvas.toDataURL("image/png");
      a.download=`profil_${(expTitle||"altimetrique").replace(/\s+/g,"_").toLowerCase()}.png`; a.click();
    };
    img.src=URL.createObjectURL(new Blob([svgData],{type:"image/svg+xml;charset=utf-8"}));
  }, [expTitle, expDesc, source, showCanopy, hasCanopy, showBuildings, hasBuildings]);

  const exportCSV = useCallback(() => {
    if (!points.length) return;
    const rows=["dist_km,altitude_m,pente_pct,longitude,latitude",...points.map(p=>`${p.dist.toFixed(4)},${p.alt.toFixed(1)},${p.slope.toFixed(2)},${p.lng.toFixed(6)},${p.lat.toFixed(6)}`)].join("\n");
    const a=document.createElement("a"); a.href=URL.createObjectURL(new Blob([rows],{type:"text/csv"}));
    a.download=`profil_${(expTitle||"altimetrique").replace(/\s+/g,"_").toLowerCase()}.csv`; a.click();
  }, [points, expTitle]);

  const inp = { fontFamily:M, fontSize:10, padding:"5px 7px", borderRadius:5, background:C.input, color:C.txt, border:`0.5px solid ${C.bdr}`, outline:"none", width:"100%", boxSizing:"border-box" };

  // ══════════════════════════════════════════════════════════════
  return (
    <div style={{ display:"flex", flexDirection:"column", width:"100%", height:"100%", minHeight:0, overflow:"hidden" }}>

      {/* ── Header ───────────────────────────────────────────── */}
      <div style={{ padding:"5px 10px", borderBottom:`0.5px solid ${C.bdr}`, flexShrink:0, display:"flex", alignItems:"center", gap:4 }}>
        {[["calc",IcMountain,"Profil"],["saved",IcStack,`Profils${profils.length?` (${profils.length})`:""}` ]].map(([v,Icon,lbl])=>(
          <button key={v} onClick={()=>setView(v)} style={{
            fontFamily:F, fontSize:10, padding:"3px 10px", borderRadius:5, cursor:"pointer",
            background: view===v ? C.acc+"18" : "transparent",
            border: `0.5px solid ${view===v ? C.acc+"55" : "transparent"}`,
            color: view===v ? C.acc : C.dim,
            display:"inline-flex", alignItems:"center", gap:5,
          }}><Icon size={12}/> {lbl}</button>
        ))}
        {source && view==="calc" && (
          <span style={{ fontSize:9, color:C.acc, fontFamily:M, marginLeft:"auto" }}>{source==="IGN"?"IGN 1m":"SRTM 30m"}</span>
        )}
      </div>

      {/* ══ MES PROFILS ══════════════════════════════════════ */}
      {view==="saved" && (
        <div style={{ flex:1, minHeight:0, overflowY:"auto", padding:"12px 12px", display:"flex", flexDirection:"column", gap:10 }}>

          {/* Quota bar */}
          <div style={{
            display:"flex", alignItems:"center", gap:10, padding:"10px 12px", borderRadius:10,
            background: profils.length>=MAX_PROFILS ? C.amb+"10" : C.acc+"09",
            border: `0.5px solid ${profils.length>=MAX_PROFILS ? C.amb+"44" : C.acc+"22"}`,
          }}>
            {/* Donut quota */}
            <div style={{ width:46, height:46, flexShrink:0 }}>
              <svg viewBox="0 0 46 46" style={{ width:"100%", height:"100%" }}>
                <circle cx="23" cy="23" r="19" fill="none" stroke={C.bdr} strokeWidth="3.5"/>
                <circle cx="23" cy="23" r="19" fill="none"
                  stroke={profils.length>=MAX_PROFILS ? C.amb : C.acc}
                  strokeWidth="3.5"
                  strokeDasharray={`${(profils.length/MAX_PROFILS)*119.4} 119.4`}
                  strokeLinecap="round" transform="rotate(-90 23 23)"/>
                <text x="23" y="27.5" textAnchor="middle" fontSize="12" fontWeight="700"
                  fill={profils.length>=MAX_PROFILS?C.amb:C.acc} fontFamily={M}>{profils.length}</text>
              </svg>
            </div>
            <div>
              <div style={{ fontSize:11, fontWeight:600, color:C.txt }}>{profils.length}/{MAX_PROFILS} profils sauvegardés</div>
              <div style={{ fontSize:9, color:C.dim, marginTop:2, lineHeight:1.4 }}>
                {profils.length===0
                  ? "Calculez un profil puis cliquez sur « Sauvegarder »."
                  : profils.length>=MAX_PROFILS
                  ? "Limite atteinte — supprimez un profil pour continuer."
                  : `${MAX_PROFILS-profils.length} emplacement${MAX_PROFILS-profils.length>1?"s":""} disponible${MAX_PROFILS-profils.length>1?"s":""}.`}
              </div>
            </div>
          </div>

          {profils.length===0 ? (
            <div style={{ flex:1, display:"flex", flexDirection:"column", alignItems:"center", justifyContent:"center", gap:12, opacity:0.45, padding:24 }}>
              <IcMountain size={44}/>
              <div style={{ fontSize:11, color:C.dim, textAlign:"center", lineHeight:1.6 }}>
                Aucun profil sauvegardé.<br/>
                Calculez votre premier tracé,<br/>puis cliquez sur <b>Sauvegarder</b>.
              </div>
            </div>
          ) : (
            <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:8 }}>
              {profils.map(p=>(
                <ProfilCard key={p.id} profil={p} C={C} onLoad={loadProfil} onDelete={deleteProfil}/>
              ))}
            </div>
          )}
        </div>
      )}

      {/* ══ CALCULER ══════════════════════════════════════════ */}
      {view==="calc" && (
        <div style={{ flex:1, minHeight:0, overflowY:"auto", overflowX:"hidden", padding:"12px 14px", display:"flex", flexDirection:"column", gap:8 }}>

          <Sep>Source de la ligne</Sep>
          <div style={{ display:"flex", gap:3 }}>
            {[["polyline",IcEdit,"Polyline"],["twopoints",IcMapPin,"2 points"],["layer",IcFolder,"Couche"]].map(([m,Icon,lbl])=>(
              <button key={m} onClick={()=>{setMode(m);onClearDraw?.();}} style={{
                fontFamily:F, flex:1, fontSize:9, padding:"5px 2px", borderRadius:5, cursor:"pointer",
                background:mode===m?C.acc+"18":"transparent",
                border:`0.5px solid ${mode===m?C.acc+"55":C.bdr}`,
                color:mode===m?C.acc:C.dim,
                display:"inline-flex", alignItems:"center", justifyContent:"center", gap:4,
              }}><Icon size={11}/> {lbl}</button>
            ))}
          </div>

          {mode==="polyline" && (
            <div style={{ fontSize:10, padding:"7px 10px", background:C.acc+"10", borderRadius:6, border:`0.5px solid ${C.acc}33`, color:C.acc, lineHeight:1.6 }}>
              Cliquez sur la carte — <b>double-clic</b> pour terminer.
              {drawPoints?.length>0 && (
                <div style={{ marginTop:4, display:"flex", alignItems:"center", gap:8 }}>
                  <span style={{ color:C.txt }}>{drawPoints.length} pt{drawPoints.length>1?"s":""}</span>
                  <button onClick={onClearDraw} style={{ fontFamily:F, fontSize:9, padding:"1px 6px", borderRadius:4, background:"transparent", border:`0.5px solid ${C.bdr}`, color:C.dim, cursor:"pointer" }}>Effacer</button>
                </div>
              )}
            </div>
          )}
          {mode==="twopoints" && (
            <div style={{ fontSize:10, padding:"7px 10px", background:C.acc+"10", borderRadius:6, border:`0.5px solid ${C.acc}33`, color:C.acc, lineHeight:1.6 }}>
              Point <b>A</b> (départ) → Point <b>B</b> (arrivée)
              {drawPoints?.length>0 && (
                <div style={{ marginTop:4, display:"flex", alignItems:"center", gap:8 }}>
                  {drawPoints.length>=1&&<span style={{ color:"#1D9E75", fontWeight:600, display:"inline-flex", alignItems:"center", gap:3 }}>A <IcCheck size={11}/></span>}
                  {drawPoints.length>=2&&<span style={{ color:"#E24B4A", fontWeight:600, display:"inline-flex", alignItems:"center", gap:3 }}>B <IcCheck size={11}/></span>}
                  {drawPoints.length<2&&<span style={{ color:C.dim }}>Point B…</span>}
                  <button onClick={onClearDraw} style={{ fontFamily:F, fontSize:9, padding:"1px 6px", borderRadius:4, background:"transparent", border:`0.5px solid ${C.bdr}`, color:C.dim, cursor:"pointer", marginLeft:"auto" }}>Effacer</button>
                </div>
              )}
            </div>
          )}
          {mode==="layer" && (
            lineLayers.length===0
              ? <div style={{ fontSize:9, color:C.amb, padding:"5px 8px", background:C.amb+"12", borderRadius:5 }}>Aucune couche LineString disponible.</div>
              : <select value={layerId} onChange={e=>setLayerId(e.target.value)}
                  style={{ fontFamily:F, fontSize:10, padding:"5px 8px", borderRadius:5, background:C.input, color:C.txt, border:`0.5px solid ${layerId?C.acc+"66":C.bdr}`, outline:"none", width:"100%" }}>
                  <option value="">— Choisir une couche —</option>
                  {lineLayers.map(l=><option key={l.id} value={l.id}>{l.name}</option>)}
                </select>
          )}

          {/* Données supplémentaires */}
          <Sep>Données supplémentaires</Sep>
          <div style={{ display:"flex", gap:5 }}>
            {[["c",showCanopy,setShowCanopy,IcLeaf,"Canopée"],["b",showBuildings,setShowBuildings,IcBuilding,"Bâti"]].map(([k,v,s,Icon,lbl])=>(
              <button key={k} onClick={()=>s(x=>!x)} style={{
                fontFamily:F, fontSize:10, flex:1, padding:"5px 0", borderRadius:6, cursor:"pointer",
                background:v?C.acc+"18":C.hover, color:v?C.acc:C.dim,
                border:`0.5px solid ${v?C.acc+"55":C.bdr}`,
                display:"inline-flex", alignItems:"center", justifyContent:"center", gap:5,
              }}><Icon size={12}/> {lbl}</button>
            ))}
          </div>

          {/* Croisement */}
          {polyLayers.length>0 && (
            <>
              <Sep>Croisement couches</Sep>
              {polyLayers.map(l=>{
                const checked=crossIds.includes(l.id);
                return (
                  <div key={l.id} onClick={()=>setCrossIds(p=>checked?p.filter(x=>x!==l.id):[...p,l.id])}
                    style={{ display:"flex", alignItems:"center", gap:8, padding:"4px 8px", borderRadius:5, cursor:"pointer", background:checked?l.color+"18":"transparent", border:`0.5px solid ${checked?l.color+"55":"transparent"}` }}>
                    <div style={{ width:8, height:8, borderRadius:2, background:l.color, flexShrink:0 }}/>
                    <span style={{ fontSize:10, color:checked?C.txt:C.mut }}>{l.name}</span>
                    {checked&&<span style={{ marginLeft:"auto", display:"flex" }}><IcCheck size={11} color={l.color}/></span>}
                  </div>
                );
              })}
            </>
          )}

          {/* Bouton calculer */}
          <button onClick={compute} disabled={loading} style={{
            fontFamily:F, fontSize:12, fontWeight:600, padding:"9px 0", borderRadius:7, width:"100%",
            background:loading?C.hover:C.acc, color:loading?C.dim:"#fff",
            border:"none", cursor:loading?"default":"pointer", opacity:loading?0.7:1,
            display:"flex", alignItems:"center", justifyContent:"center", gap:7,
          }}><IcMountain size={14}/> {loading?"Calcul en cours…":"Calculer le profil"}</button>

          {error && <div style={{ fontSize:10, padding:"6px 8px", borderRadius:5, background:C.red+"15", border:`0.5px solid ${C.red}44`, color:C.red, display:"flex", gap:5 }}><IcAlert size={12} style={{ flexShrink:0, marginTop:1 }}/> <span>{error}</span></div>}

          {/* ── Résultats ──────────────────────────────────── */}
          {points.length>0 && (<>

            <Sep>Profil</Sep>
            <div style={{ background:C.hover, borderRadius:8, border:`0.5px solid ${C.bdr}`, padding:"6px 2px 2px", width:"100%", boxSizing:"border-box" }}>
              <ProfilSVG points={points} hoverIdx={hoverIdx} onHover={setHoverIdx} crossLayers={crossData}
                showCanopy={showCanopy&&hasCanopy} showBuildings={showBuildings&&hasBuildings}/>
            </div>

            {/* Légende */}
            <div style={{ display:"flex", gap:5, flexWrap:"wrap" }}>
              {showCanopy&&hasCanopy&&<div style={{ display:"flex", alignItems:"center", gap:3, fontSize:9, color:C.dim }}><div style={{ width:14,height:7,borderRadius:2,background:"#4CAF5033",border:"1.5px solid #4CAF50" }}/><span>Canopée</span></div>}
              {showBuildings&&hasBuildings&&<div style={{ display:"flex", alignItems:"center", gap:3, fontSize:9, color:C.dim }}><div style={{ width:6,height:9,borderRadius:1,background:"#888" }}/><span>Bâti</span></div>}
              {PENTE_SEUILS.map(s=>(
                <div key={s.id} style={{ display:"flex", alignItems:"center", gap:3, fontSize:9, color:C.dim }}>
                  <div style={{ width:13, height:3, borderRadius:2, background:s.color }}/><span>{s.label}</span>
                </div>
              ))}
            </div>
            {crossData.length>0&&<div style={{ display:"flex", gap:5, flexWrap:"wrap" }}>{crossData.map((cl,i)=>(
              <div key={i} style={{ display:"flex", alignItems:"center", gap:3, fontSize:9, color:C.dim }}>
                <div style={{ width:9,height:9,borderRadius:2,background:cl.color,opacity:0.5 }}/><span>{cl.label}</span>
              </div>
            ))}</div>}

            {/* Stats */}
            <Sep>Statistiques</Sep>
            <div style={{ display:"flex", gap:5 }}>
              <StatBox label="Distance" value={stats.dist<1?(stats.dist*1000).toFixed(0):stats.dist.toFixed(2)} unit={stats.dist<1?"m":"km"} color={C.acc}/>
              <StatBox label="Alt min" value={stats.altMin} unit="m" color={C.blu}/>
              <StatBox label="Alt max" value={stats.altMax} unit="m" color={C.amb}/>
              <StatBox label="Alt moy" value={stats.altMoy} unit="m"/>
            </div>
            <div style={{ display:"flex", gap:5 }}>
              <StatBox label="D+ cumulé" value={stats.dPlus} unit="m" color={C.acc}/>
              <StatBox label="D− cumulé" value={stats.dMinus} unit="m" color={C.red}/>
              <StatBox label="Pente max" value={stats.slopeMax} unit="%" color={penteColor(parseFloat(stats.slopeMax))}/>
              <StatBox label="Pente moy" value={stats.slopeMoy} unit="%"/>
            </div>

            {/* Sauvegarder */}
            <Sep>Sauvegarder</Sep>
            <input value={expTitle} onChange={e=>setExpTitle(e.target.value)} placeholder="Titre du profil (ex : Loire à Vélo)" style={inp}/>
            <input value={expDesc} onChange={e=>setExpDesc(e.target.value)} placeholder="Description (optionnelle)" style={inp}/>
            <div style={{ display:"flex", gap:6, alignItems:"center" }}>
              <button onClick={saveProfil} disabled={profils.length>=MAX_PROFILS} style={{
                fontFamily:F, fontSize:11, fontWeight:600, flex:1, padding:"8px 0", borderRadius:6,
                background:profils.length>=MAX_PROFILS?C.hover:"#1D9E7518",
                border:`0.5px solid ${profils.length>=MAX_PROFILS?C.bdr:"#1D9E7555"}`,
                color:profils.length>=MAX_PROFILS?C.dim:"#1D9E75",
                cursor:profils.length>=MAX_PROFILS?"default":"pointer",
                display:"inline-flex", alignItems:"center", justifyContent:"center", gap:6,
              }}><IcSave size={13}/> Sauvegarder ({profils.length}/{MAX_PROFILS})</button>
              <button onClick={()=>setView("saved")} style={{
                fontFamily:F, fontSize:10, padding:"8px 10px", borderRadius:6, cursor:"pointer",
                background:"transparent", border:`0.5px solid ${C.bdr}`, color:C.dim, display:"flex", alignItems:"center",
              }}><IcStack size={14}/></button>
            </div>
            {saveMsg&&<div style={{ fontSize:10, padding:"5px 8px", borderRadius:5, background:saveMsg.type==="ok"?"#1D9E7515":C.amb+"15", border:`0.5px solid ${saveMsg.type==="ok"?"#1D9E7544":C.amb+"44"}`, color:saveMsg.type==="ok"?"#1D9E75":C.amb }}>{saveMsg.text}</div>}

            {/* Export */}
            <Sep>Export</Sep>
            <div style={{ fontSize:9, color:C.dim, padding:"3px 7px", background:C.hover, borderRadius:5 }}>
              Source : <span style={{ color:C.acc }}>OpenMapAgents · {source==="IGN"?"IGN RGE Alti 1m":"SRTM GEE 30m"}</span>
            </div>
            <div style={{ display:"flex", gap:6 }}>
              <button onClick={exportPNG} style={{ fontFamily:F, fontSize:10, flex:1, padding:"7px 0", borderRadius:6, background:C.acc+"18", border:`0.5px solid ${C.acc}55`, color:C.acc, cursor:"pointer", display:"inline-flex", alignItems:"center", justifyContent:"center", gap:5 }}><IcImage size={12}/> PNG</button>
              <button onClick={exportCSV} style={{ fontFamily:F, fontSize:10, flex:1, padding:"7px 0", borderRadius:6, background:"transparent", border:`0.5px solid ${C.bdr}`, color:C.mut, cursor:"pointer", display:"inline-flex", alignItems:"center", justifyContent:"center", gap:5 }}><IcFile size={12}/> CSV</button>
            </div>

          </>)}
          <div style={{ height:8 }}/>
        </div>
      )}
    </div>
  );
}
