import { useState, useEffect, useCallback, useRef } from "react";
import { getTheme } from "../theme";
import { F, M } from "../config";
import { geocodeAddress, computeRoute, computeIsochrone } from "../utils/routing";
import { Btn } from "./ui";

function AddressInput({ label, value, onChange, onSelect, placeholder, color }) {
  const C = getTheme();
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
    <div style={{ position: "relative" }}>
      <div style={{ fontSize: 10, color: C.dim, marginBottom: 2, display: "flex", alignItems: "center", gap: 4 }}>
        <div style={{ width: 8, height: 8, borderRadius: "50%", background: color || C.acc, flexShrink: 0 }} />
        {label}
      </div>
      <input value={value} onChange={e => search(e.target.value)}
        onFocus={() => setFocused(true)} onBlur={() => setTimeout(() => setFocused(false), 200)}
        placeholder={placeholder}
        style={{ fontFamily: F, fontSize: 12, padding: "6px 10px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", width: "100%", boxSizing: "border-box" }} />
      {focused && suggestions.length > 0 && (
        <div style={{ position: "absolute", top: "100%", left: 0, right: 0, zIndex: 100, marginTop: 2, background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 6, maxHeight: 150, overflowY: "auto", boxShadow: "0 4px 12px rgba(0,0,0,0.2)" }}>
          {suggestions.map((s, i) => (
            <div key={i} onClick={() => { onSelect(s); setSuggestions([]); onChange(s.label.split(",").slice(0, 2).join(",")); }}
              style={{ padding: "6px 10px", fontSize: 11, color: C.txt, cursor: "pointer", borderBottom: `0.5px solid ${C.bdr}` }}
              onMouseEnter={e => e.currentTarget.style.background = C.hover}
              onMouseLeave={e => e.currentTarget.style.background = "transparent"}>
              {s.label.length > 60 ? s.label.slice(0, 60) + "..." : s.label}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default function RoutePanel({ mode, profile, onProfileChange, onResult, onClose, onSetMapClick, onMarkers, onAddLayer }) {
  const C = getTheme();
  const [originText, setOriginText] = useState("");
  const [destText, setDestText] = useState("");
  const [originCoord, setOriginCoord] = useState(null);
  const [destCoord, setDestCoord] = useState(null);
  const [isoTime, setIsoTime] = useState(10);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [clickTarget, setClickTarget] = useState(null);
  const [lastGeoJSON, setLastGeoJSON] = useState(null);

  // ── Drag ──────────────────────────────────────────────────────
  const [pos, setPos] = useState({ x: null, y: null });
  const dragRef       = useRef({ dragging: false, ox: 0, oy: 0 });
  const panelRef      = useRef(null);

  const onDragStart = useCallback((e) => {
    if (e.button !== 0) return;
    e.preventDefault();
    const rect = panelRef.current?.getBoundingClientRect();
    dragRef.current = { dragging: true, ox: e.clientX - (rect?.left ?? 0), oy: e.clientY - (rect?.top ?? 0) };
    const onMove = (ev) => {
      if (!dragRef.current.dragging) return;
      setPos({ x: ev.clientX - dragRef.current.ox, y: ev.clientY - dragRef.current.oy });
    };
    const onUp = () => {
      dragRef.current.dragging = false;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);

  const exportGJ = (gj, name) => {
    if (!gj) return;
    const blob = new Blob([JSON.stringify(gj, null, 2)], { type: "application/json" });
    const a = document.createElement("a"); a.href = URL.createObjectURL(blob);
    a.download = `${name.replace(/\s+/g, "_")}.geojson`; a.click();
  };

  // Notify parent of marker positions for map display
  useEffect(() => {
    onMarkers?.(originCoord, destCoord);
  }, [originCoord, destCoord, onMarkers]); // "origin" | "dest" | "iso"

  // Register click handler for map clicks
  useEffect(() => {
    if (onSetMapClick) {
      if (clickTarget) {
        onSetMapClick((lng, lat) => {
          const coord = [lng, lat];
          const label = `${lat.toFixed(5)}, ${lng.toFixed(5)}`;
          if (clickTarget === "origin" || clickTarget === "iso") {
            setOriginCoord(coord); setOriginText(label);
          } else if (clickTarget === "dest") {
            setDestCoord(coord); setDestText(label);
          }
          setClickTarget(null);
        });
      } else {
        onSetMapClick(null);
      }
    }
  }, [clickTarget, onSetMapClick]);

  const calculate = async () => {
    setError(null); setLoading(true); setResult(null);
    try {
      // Auto-geocode if user typed text but didn't select a suggestion
      let oc = originCoord;
      let dc = destCoord;
      if (!oc && originText.trim()) {
        const results = await geocodeAddress(originText);
        if (results.length) { oc = [results[0].lon, results[0].lat]; setOriginCoord(oc); setOriginText(results[0].label.split(",").slice(0, 2).join(",")); }
      }
      if (mode === "route" && !dc && destText.trim()) {
        const results = await geocodeAddress(destText);
        if (results.length) { dc = [results[0].lon, results[0].lat]; setDestCoord(dc); setDestText(results[0].label.split(",").slice(0, 2).join(",")); }
      }

      if (mode === "route") {
        if (!oc || !dc) throw new Error("Impossible de geocoder les adresses");
        const gj = await computeRoute([oc, dc], profile);
        setResult(gj.metadata);
        setLastGeoJSON(gj);
        onResult(gj);
      } else {
        if (!oc) throw new Error("Impossible de geocoder l'adresse");
        const gj = await computeIsochrone(oc, isoTime, profile);
        setResult({ breaks: gj.metadata.breaks, profile });
        setLastGeoJSON(gj);
        onResult(gj);
      }
    } catch (e) { setError(e.message); }
    setLoading(false);
  };

  return (
    <div ref={panelRef} style={{
      position: "fixed",
      ...(pos.x !== null ? { left: pos.x, top: pos.y } : { top: 50, left: 10 }),
      zIndex: 25, width: 300,
      background: C.card, borderRadius: 10, border: `0.5px solid ${C.bdr}`,
      boxShadow: "0 4px 20px rgba(0,0,0,0.25)", padding: 14,
      display: "flex", flexDirection: "column", gap: 10,
      userSelect: "none",
    }}>
      {/* Header — zone de drag */}
      <div onMouseDown={onDragStart} style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "grab" }}>
        <div style={{ fontSize: 13, fontWeight: 500, color: C.txt, display: "flex", alignItems: "center", gap: 6 }}>
          <span style={{ fontSize: 11, color: C.dim, letterSpacing: 2 }}>⠿</span>
          {mode === "route" ? "Itinéraire" : "Isochrone"}
        </div>
        <button onClick={onClose} style={{ fontSize: 12, background: "none", border: "none", color: C.dim, cursor: "pointer", fontFamily: F }}>✕</button>
      </div>

      {/* Profile */}
      <div style={{ display: "flex", gap: 3 }}>
        {[["foot", "A pied"], ["bike", "Velo"], ["car", "Voiture"]].map(([k, label]) => (
          <button key={k} onClick={() => onProfileChange(k)} style={{
            fontFamily: F, fontSize: 11, padding: "4px 10px", borderRadius: 5, flex: 1,
            background: profile === k ? C.acc + "18" : "transparent",
            border: `0.5px solid ${profile === k ? C.acc + "55" : C.bdr}`,
            color: profile === k ? C.acc : C.dim, cursor: "pointer",
          }}>{label}</button>
        ))}
      </div>

      {/* Origin */}
      <AddressInput label={mode === "route" ? "Origine" : "Centre"} value={originText} onChange={setOriginText}
        onSelect={s => setOriginCoord([s.lon, s.lat])} placeholder="Adresse ou clic carte..." color="#378ADD" />
      <button onClick={() => setClickTarget(mode === "route" ? "origin" : "iso")} style={{
        fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4,
        background: clickTarget === "origin" || clickTarget === "iso" ? C.blu + "20" : "transparent",
        border: `0.5px solid ${clickTarget ? C.blu + "55" : C.bdr}`,
        color: clickTarget ? C.blu : C.dim, cursor: "pointer",
      }}>{clickTarget ? "Cliquez sur la carte..." : "Placer sur la carte"}</button>

      {/* Destination (route only) */}
      {mode === "route" && (
        <>
          <AddressInput label="Destination" value={destText} onChange={setDestText}
            onSelect={s => setDestCoord([s.lon, s.lat])} placeholder="Adresse ou clic carte..." color="#E24B4A" />
          <button onClick={() => setClickTarget("dest")} style={{
            fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4,
            background: clickTarget === "dest" ? C.red + "20" : "transparent",
            border: `0.5px solid ${clickTarget === "dest" ? C.red + "55" : C.bdr}`,
            color: clickTarget === "dest" ? C.red : C.dim, cursor: "pointer",
          }}>{clickTarget === "dest" ? "Cliquez sur la carte..." : "Placer sur la carte"}</button>
        </>
      )}

      {/* Isochrone time */}
      {mode === "isochrone" && (
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ fontSize: 11, color: C.dim }}>Temps</span>
          <select value={isoTime} onChange={e => setIsoTime(parseInt(e.target.value))}
            style={{ fontFamily: F, fontSize: 11, padding: "4px 8px", borderRadius: 6, background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none", flex: 1 }}>
            <option value="5">5 min</option>
            <option value="10">10 min</option>
            <option value="15">15 min</option>
            <option value="20">20 min</option>
            <option value="30">30 min</option>
            <option value="45">45 min</option>
            <option value="60">60 min</option>
          </select>
        </div>
      )}

      {/* Calculate */}
      <button onClick={calculate} disabled={loading} style={{
        fontFamily: F, fontSize: 12, fontWeight: 500, padding: "8px 16px", borderRadius: 6,
        background: C.acc, color: "#fff", border: "none", cursor: loading ? "default" : "pointer",
        opacity: loading ? 0.6 : 1,
      }}>{loading ? "Calcul..." : "Calculer"}</button>

      {/* Error */}
      {error && <div style={{ fontSize: 11, color: C.red, padding: "4px 8px", background: C.red + "10", borderRadius: 4 }}>{error}</div>}

      {/* Result summary */}
      {result && mode === "route" && (
        <div style={{ background: C.hover, borderRadius: 6, padding: 8, border: `0.5px solid ${C.bdr}` }}>
          <div style={{ fontSize: 13, fontWeight: 500, color: C.acc, marginBottom: 4 }}>
            {result.distance_km} km — {result.duration_min} min
          </div>
          {result.steps?.slice(0, 5).map((s, i) => (
            <div key={i} style={{ fontSize: 10, color: C.mut, padding: "1px 0" }}>
              {s.instruction}
            </div>
          ))}
          {result.steps?.length > 5 && <div style={{ fontSize: 10, color: C.dim }}>... +{result.steps.length - 5} etapes</div>}
          <div style={{ display: "flex", gap: 4, marginTop: 6 }}>
            <button onClick={() => onAddLayer?.(lastGeoJSON, `Route ${profile} ${result.distance_km}km`, "route")} style={{
              fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4,
              background: C.acc + "18", color: C.acc, border: `0.5px solid ${C.acc}33`, cursor: "pointer",
            }}>Ajouter comme couche</button>
            <button onClick={() => exportGJ(lastGeoJSON, `route_${profile}`)} style={{
              fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4,
              background: C.blu + "18", color: C.blu, border: `0.5px solid ${C.blu}33`, cursor: "pointer",
            }}>Export GeoJSON</button>
          </div>
        </div>
      )}

      {result && mode === "isochrone" && (
        <div style={{ background: C.hover, borderRadius: 6, padding: 8, border: `0.5px solid ${C.bdr}` }}>
          <div style={{ fontSize: 12, color: C.acc, marginBottom: 4 }}>Isochrones: {result.breaks.join(", ")} min ({result.profile})</div>
          <div style={{ display: "flex", gap: 4 }}>
            <button onClick={() => onAddLayer?.(lastGeoJSON, `Isochrone ${result.breaks.join("-")}min ${profile}`, "isochrone")} style={{
              fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4,
              background: C.acc + "18", color: C.acc, border: `0.5px solid ${C.acc}33`, cursor: "pointer",
            }}>Ajouter comme couche</button>
            <button onClick={() => exportGJ(lastGeoJSON, `isochrone_${profile}_${result.breaks.join("-")}min`)} style={{
              fontFamily: F, fontSize: 10, padding: "3px 8px", borderRadius: 4,
              background: C.blu + "18", color: C.blu, border: `0.5px solid ${C.blu}33`, cursor: "pointer",
            }}>Export GeoJSON</button>
          </div>
        </div>
      )}

      <div style={{ fontSize: 9, color: C.dim }}>Powered by Mapbox Directions + Isochrone API</div>
    </div>
  );
}
