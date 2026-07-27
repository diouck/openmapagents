import { useEffect, useRef, useState } from "react";
import { useThemeContext } from "../theme";
import { MAP_STYLES } from "../config";
import maplibregl from "maplibre-gl";

export default function MiniMap({ center, zoom, mapStyle }) {
  const C = useThemeContext();
  const containerRef = useRef(null);
  const mapRef       = useRef(null);
  const markerRef    = useRef(null);
  const [failed, setFailed] = useState(false);

  const getMiniStyle = (style) => {
    if (!style) return MAP_STYLES.positron;
    if (typeof style === "string")
      return style.includes("dark") ? MAP_STYLES.positron : style;
    if (typeof style === "object")
      return { version:8, sources:{}, layers:[], ...style, projection:{type:"mercator"} };
    return MAP_STYLES.positron;
  };

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;
    try {
      const mini = new maplibregl.Map({
        container:           containerRef.current,
        style:               getMiniStyle(mapStyle) || MAP_STYLES.positron,
        center:              center || [0, 0],
        zoom:                Math.max((zoom || 12) - 5, 1),
        interactive:         false,
        attributionControl:  false,
        failIfMajorPerformanceCaveat: true,
      });
      mapRef.current = mini;
      const el = document.createElement("div");
      el.style.cssText = `width:20px;height:16px;border:2px solid ${C.acc};border-radius:3px;background:${C.acc}26;pointer-events:none;`;
      markerRef.current = new maplibregl.Marker({ element: el })
        .setLngLat(center || [0, 0])
        .addTo(mini);
      return () => { mini.remove(); mapRef.current = null; };
    } catch (e) {
      console.warn("MiniMap WebGL indisponible:", e);
      setFailed(true);
    }
  }, []);

  useEffect(() => {
    if (!mapRef.current || !mapStyle) return;
    try { mapRef.current.setStyle(getMiniStyle(mapStyle)); }
    catch (e) { console.warn("MiniMap style error:", e); }
  }, [mapStyle]);

  useEffect(() => {
    if (!mapRef.current || !center) return;
    mapRef.current.jumpTo({ center, zoom: Math.max((zoom || 12) - 5, 1) });
    markerRef.current?.setLngLat(center);
  }, [center, zoom]);

  // Si WebGL indisponible → ne rien afficher (pas de crash)
  if (failed) return null;

  return (
    <div style={{
      position:"absolute", bottom:30, right:10, zIndex:10,
      width:150, height:110, borderRadius:8, overflow:"hidden",
      border:`1.5px solid ${C.acc}44`,
      boxShadow:"0 2px 8px rgba(0,0,0,0.3)",
    }}>
      <div ref={containerRef} style={{ width:"100%", height:"100%" }} />
    </div>
  );
}