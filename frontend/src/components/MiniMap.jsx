import { useEffect, useRef } from "react";
import { useThemeContext } from "../theme";
import maplibregl from "maplibre-gl";

/**
 * MiniMap — small overview map showing current viewport in context.
 * Renders a tiny MapLibre instance synced with the main map's center/zoom.
 */
export default function MiniMap({ center, zoom, mapStyle }) {
  const C = useThemeContext();
  const containerRef = useRef(null);
  const mapRef = useRef(null);
  const markerRef = useRef(null);

  // Initialize mini map
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const miniMap = new maplibregl.Map({
      container: containerRef.current,
      style: mapStyle || "https://tiles.openfreemap.org/styles/positron",
      center: center || [0, 0],
      zoom: Math.max((zoom || 12) - 5, 1),
      interactive: false,
      attributionControl: false,
    });

    mapRef.current = miniMap;

    // Viewport rectangle marker (CSS div)
    const el = document.createElement("div");
    el.style.cssText = `width:24px;height:18px;border:2px solid #1D9E75;border-radius:3px;background:rgba(29,158,117,0.15);pointer-events:none;`;
    const marker = new maplibregl.Marker({ element: el }).setLngLat(center || [0, 0]).addTo(miniMap);
    markerRef.current = marker;

    return () => { miniMap.remove(); mapRef.current = null; };
  }, []);

  // Update style when main map style changes
  useEffect(() => {
    if (!mapRef.current) return;
    // Use positron for mini map (lighter, better for overview)
    const miniStyle = mapStyle?.includes("dark")
      ? "https://tiles.openfreemap.org/styles/positron"
      : mapStyle || "https://tiles.openfreemap.org/styles/positron";
    try { mapRef.current.setStyle(miniStyle); } catch {}
  }, [mapStyle]);

  // Sync position
  useEffect(() => {
    if (!mapRef.current || !center) return;
    const miniZoom = Math.max((zoom || 12) - 5, 1);
    mapRef.current.jumpTo({ center, zoom: miniZoom });
    markerRef.current?.setLngLat(center);
  }, [center, zoom]);

  return (
    <div style={{
      position: "absolute", bottom: 30, right: 10, zIndex: 10,
      width: 150, height: 110, borderRadius: 8, overflow: "hidden",
      border: `1.5px solid ${C.acc}44`,
      boxShadow: "0 2px 8px rgba(0,0,0,0.2)",
    }}>
      <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
    </div>
  );
}
