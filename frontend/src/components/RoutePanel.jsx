import { useState, useRef } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import { geocodeAddress } from "../utils/routing";

// ── Champ adresse avec autocomplétion ──────────────────────────
function AddressInput({ label, value, onChange, onCoord, placeholder, color, pickActive, onPickToggle }) {
  const C = useThemeContext();
  const [suggestions, setSuggestions] = useState([]);
  const [focused, setFocused] = useState(false);
  const timer = useRef(null);

  const search = (q) => {
    onChange(q);
    clearTimeout(timer.current);
    if (q.length < 3) { setSuggestions([]); return; }
    timer.current = setTimeout(async () => {
      const results = await geocodeAddress(q);
      setSuggestions(results);
    }, 400);
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <div style={{ fontSize: 10, color: C.dim, display: "flex", alignItems: "center", gap: 4 }}>
        <div style={{ width: 8, height: 8, borderRadius: "50%", background: color || C.acc, flexShrink: 0 }} />
        {label}
      </div>
      <div style={{ display: "flex", gap: 6 }}>
        <div style={{ position: "relative", flex: 1 }}>
          <input
            value={value}
            onChange={e => search(e.target.value)}
            onFocus={() => setFocused(true)}
            onBlur={() => setTimeout(() => setFocused(false), 200)}
            placeholder={placeholder}
            style={{
              fontFamily: F, fontSize: 11, padding: "7px 10px", borderRadius: 7,
              background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`,
              outline: "none", width: "100%", boxSizing: "border-box",
            }}
          />
          {focused && suggestions.length > 0 && (
            <div style={{
              position: "absolute", top: "100%", left: 0, right: 0, zIndex: 200,
              marginTop: 2, background: C.card, border: `0.5px solid ${C.bdr}`,
              borderRadius: 7, maxHeight: 160, overflowY: "auto",
              boxShadow: "0 4px 16px rgba(0,0,0,0.28)",
            }}>
              {suggestions.map((s, i) => (
                <div key={i}
                  onClick={() => {
                    onChange(s.label.split(",").slice(0, 2).join(","));
                    onCoord([s.lon, s.lat]);
                    setSuggestions([]);
                  }}
                  style={{ padding: "7px 10px", fontSize: 11, color: C.txt, cursor: "pointer", borderBottom: `0.5px solid ${C.bdr}` }}
                  onMouseEnter={e => e.currentTarget.style.background = C.hover}
                  onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                >
                  {s.label.length > 60 ? s.label.slice(0, 60) + "…" : s.label}
                </div>
              ))}
            </div>
          )}
        </div>
        {/* Bouton clic carte */}
        <button
          onClick={onPickToggle}
          title="Placer sur la carte"
          style={{
            fontFamily: F, fontSize: 13, padding: "0 10px", borderRadius: 7, flexShrink: 0,
            border: `0.5px solid ${pickActive ? (color || C.acc) + "88" : C.bdr}`,
            background: pickActive ? (color || C.acc) + "22" : "transparent",
            color: pickActive ? (color || C.acc) : C.dim, cursor: "pointer",
          }}
        >
          📍
        </button>
      </div>
      {pickActive && (
        <div style={{ fontSize: 10, color: color || C.acc, padding: "5px 8px", background: (color || C.acc) + "12", borderRadius: 6 }}>
          Cliquez sur la carte pour placer le point
        </div>
      )}
    </div>
  );
}

// ── Panel principal — intégré dans la sidebar, sans position fixed ──
export default function RoutePanel({
  mode,
  // Route
  routeOrigin, setRouteOrigin, routeDest, setRouteDest,
  routeOriginCoord, setRouteOriginCoord, routeDestCoord, setRouteDestCoord,
  // Isochrone
  isoCenter, setIsoCenter, isoCenterCoord, setIsoCenterCoord,
  isoTime, setIsoTime,
  // Commun
  profile, onProfileChange,
  pickMode, setPickMode,
  routeLayer, isoLayer,
  loading,
  onCompute,
  onClear,
  onAddLayer,
  setRouteMarkers,
}) {
  const C = useThemeContext();

  const labelSt = {
    fontSize: 10, fontWeight: 500, color: C.dim,
    textTransform: "uppercase", letterSpacing: "0.05em",
  };

  // Mise à jour marqueurs quand coordonnées changent
  const updateMarkers = (oc, dc) => {
    const features = [];
    if (oc) features.push({ type:"Feature", geometry:{ type:"Point", coordinates:oc }, properties:{ type:"origin", label:"A" } });
    if (dc) features.push({ type:"Feature", geometry:{ type:"Point", coordinates:dc }, properties:{ type:"dest",   label:"B" } });
    setRouteMarkers(features.length ? { type:"FeatureCollection", features } : null);
  };

  return (
    <div style={{ display:"flex", flexDirection:"column", width:"100%", height:"100%", minHeight:0, overflow:"hidden" }}>
      <div style={{ flex:1, minHeight:0, overflowY:"auto", overflowX:"hidden", padding:"12px 14px", display:"flex", flexDirection:"column", gap:12 }}>

        {/* Transport */}
        <div>
          <div style={labelSt}>Transport</div>
          <div style={{ display:"flex", gap:4, marginTop:4 }}>
            {[["foot","À pied"],["bike","Vélo"],["car","Voiture"]].map(([k,lbl]) => (
              <button key={k} onClick={() => onProfileChange(k)} style={{
                fontFamily:F, flex:1, padding:"6px 4px", borderRadius:7, cursor:"pointer", fontSize:11,
                border:`0.5px solid ${profile===k ? C.acc+"55" : C.bdr}`,
                background: profile===k ? C.acc+"18" : "transparent",
                color: profile===k ? C.acc : C.mut,
              }}>{lbl}</button>
            ))}
          </div>
        </div>

        {/* ── Route ── */}
        {mode === "route" && (<>
          <AddressInput
            label="Départ" value={routeOrigin} color="#378ADD"
            onChange={setRouteOrigin}
            onCoord={c => { setRouteOriginCoord(c); updateMarkers(c, routeDestCoord); }}
            placeholder="Adresse ou lat, lon…"
            pickActive={pickMode === "origin"}
            onPickToggle={() => setPickMode(p => p === "origin" ? null : "origin")}
          />
          <AddressInput
            label="Arrivée" value={routeDest} color="#E24B4A"
            onChange={setRouteDest}
            onCoord={c => { setRouteDestCoord(c); updateMarkers(routeOriginCoord, c); }}
            placeholder="Adresse ou lat, lon…"
            pickActive={pickMode === "dest"}
            onPickToggle={() => setPickMode(p => p === "dest" ? null : "dest")}
          />
        </>)}

        {/* ── Isochrone ── */}
        {mode === "isochrone" && (<>
          <AddressInput
            label="Centre" value={isoCenter} color="#378ADD"
            onChange={setIsoCenter}
            onCoord={c => {
              setIsoCenterCoord(c);
              setRouteMarkers({ type:"FeatureCollection", features:[{ type:"Feature", geometry:{ type:"Point", coordinates:c }, properties:{ type:"origin", label:"●" } }] });
            }}
            placeholder="Adresse ou lat, lon…"
            pickActive={pickMode === "iso"}
            onPickToggle={() => setPickMode(p => p === "iso" ? null : "iso")}
          />
          <div>
            <div style={labelSt}>Temps de trajet</div>
            <div style={{ display:"flex", alignItems:"center", gap:8, marginTop:4 }}>
              <input type="range" min={5} max={60} step={5} value={isoTime}
                onChange={e => setIsoTime(Number(e.target.value))} style={{ flex:1, height:3 }} />
              <span style={{ fontFamily:M, fontSize:12, color:C.txt, minWidth:40 }}>{isoTime} min</span>
            </div>
          </div>
        </>)}

        {/* Bouton calculer */}
        <button onClick={onCompute} disabled={loading} style={{
          fontFamily:F, fontSize:12, fontWeight:500, padding:"8px 16px", borderRadius:7,
          background:C.acc, color:"#fff", border:"none",
          cursor: loading ? "default" : "pointer", opacity: loading ? 0.6 : 1,
        }}>
          {loading ? "Calcul en cours…" : mode==="route" ? "Calculer l'itinéraire" : "Calculer l'isochrone"}
        </button>

        {/* Résultat itinéraire */}
        {routeLayer && mode === "route" && (
          <div style={{ background:C.hover, borderRadius:8, padding:"10px 12px", border:`0.5px solid ${C.bdr}`, display:"flex", flexDirection:"column", gap:6 }}>
            <div style={{ fontSize:12, fontWeight:600, color:C.acc }}>
              {routeLayer.metadata?.distance_km?.toFixed(1)} km · {routeLayer.metadata?.duration_min?.toFixed(0)} min
            </div>
            {routeLayer.metadata?.steps?.slice(0,4).map((s,i) => (
              <div key={i} style={{ fontSize:10, color:C.mut }}>{s.instruction}</div>
            ))}
            {routeLayer.metadata?.steps?.length > 4 && (
              <div style={{ fontSize:10, color:C.dim }}>… +{routeLayer.metadata.steps.length-4} étapes</div>
            )}
            <div style={{ display:"flex", gap:4, marginTop:2 }}>
              <button onClick={() => onAddLayer(routeLayer, `Route ${profile} ${routeLayer.metadata?.distance_km?.toFixed(1)}km`, "route")}
                style={{ fontFamily:F, fontSize:10, flex:1, padding:"5px 0", borderRadius:6, border:`0.5px solid ${C.acc}55`, background:C.acc+"18", color:C.acc, cursor:"pointer" }}>
                Ajouter comme couche
              </button>
              <button onClick={onClear}
                style={{ fontFamily:F, fontSize:10, flex:1, padding:"5px 0", borderRadius:6, border:`0.5px solid ${C.bdr}`, background:"transparent", color:C.mut, cursor:"pointer" }}>
                Effacer
              </button>
            </div>
          </div>
        )}

        {/* Résultat isochrone */}
        {isoLayer && mode === "isochrone" && (
          <div style={{ background:C.hover, borderRadius:8, padding:"10px 12px", border:`0.5px solid ${C.bdr}`, display:"flex", flexDirection:"column", gap:6 }}>
            <div style={{ fontSize:12, fontWeight:600, color:C.acc }}>
              Isochrone {isoTime} min ({profile})
            </div>
            <div style={{ display:"flex", gap:4 }}>
              <button onClick={() => onAddLayer(isoLayer, `Isochrone ${isoTime}min ${profile}`, "isochrone")}
                style={{ fontFamily:F, fontSize:10, flex:1, padding:"5px 0", borderRadius:6, border:`0.5px solid ${C.acc}55`, background:C.acc+"18", color:C.acc, cursor:"pointer" }}>
                Ajouter comme couche
              </button>
              <button onClick={onClear}
                style={{ fontFamily:F, fontSize:10, flex:1, padding:"5px 0", borderRadius:6, border:`0.5px solid ${C.bdr}`, background:"transparent", color:C.mut, cursor:"pointer" }}>
                Effacer
              </button>
            </div>
          </div>
        )}

        <div style={{ fontSize:9, color:C.dim }}>Powered by OpenRouteService API</div>
      </div>
    </div>
  );
}
