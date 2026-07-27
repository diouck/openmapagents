import { useState, useEffect, useCallback, useRef } from "react";
import { getTheme } from "../theme";
import { API, F, CHAT_INPUT_ENABLED } from "../config";
import { Badge, Spinner } from "./ui";
import { IcMap, IcSatellite, IcGlobe, IcRoute, IcBarChart, IcVenn, IcServer,
  IcTrendingUp, IcBuilding, IcSparkles, IcLock } from "../icons";

import VoiceInput from "./VoiceInput";

// ─── Lightweight Markdown renderer ───────────────────────────
function MarkdownText({ text, color }) {
  if (!text) return null;
  const lines = text.split("\n");
  const elements = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    const numMatch    = trimmed.match(/^(\d+)\.\s+(.+)/);
    const bulletMatch = trimmed.match(/^[-•]\s+(.+)/);

    if (numMatch) {
      elements.push(
        <div key={i} style={{ display: "flex", gap: 6, padding: "3px 0", alignItems: "flex-start" }}>
          <span style={{ color: "var(--md-num, #1D9E75)", fontWeight: 500, fontSize: 12, minWidth: 18, textAlign: "right", flexShrink: 0 }}>{numMatch[1]}.</span>
          <span style={{ fontSize: 12, lineHeight: 1.5 }}>{renderInline(numMatch[2], color)}</span>
        </div>
      );
    } else if (bulletMatch) {
      elements.push(
        <div key={i} style={{ display: "flex", gap: 6, padding: "1px 0 1px 20px", alignItems: "flex-start" }}>
          <span style={{ color: "var(--md-dim, #888)", fontSize: 8, marginTop: 4 }}>●</span>
          <span style={{ fontSize: 11, lineHeight: 1.5, color: "var(--md-sub, #999)" }}>{renderInline(bulletMatch[1], color)}</span>
        </div>
      );
    } else if (trimmed === "") {
      elements.push(<div key={i} style={{ height: 6 }} />);
    } else {
      elements.push(
        <div key={i} style={{ fontSize: 13, lineHeight: 1.6, padding: "1px 0" }}>{renderInline(trimmed, color)}</div>
      );
    }
    i++;
  }

  return <div style={{ color }}>{elements}</div>;
}

function renderInline(text, color) {
  const parts = [];
  let remaining = text;
  let key = 0;

  while (remaining.length > 0) {
    const boldMatch = remaining.match(/^(.*?)\*\*(.+?)\*\*(.*)/s);
    const codeMatch = remaining.match(/^(.*?)`(.+?)`(.*)/s);

    if (boldMatch && (!codeMatch || boldMatch.index <= codeMatch.index)) {
      if (boldMatch[1]) parts.push(<span key={key++}>{boldMatch[1]}</span>);
      parts.push(<span key={key++} style={{ fontWeight: 500, color: color || "inherit" }}>{boldMatch[2]}</span>);
      remaining = boldMatch[3];
    } else if (codeMatch) {
      if (codeMatch[1]) parts.push(<span key={key++}>{codeMatch[1]}</span>);
      parts.push(<span key={key++} style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 11, padding: "1px 4px", borderRadius: 3, background: "rgba(0,0,0,0.08)" }}>{codeMatch[2]}</span>);
      remaining = codeMatch[3];
    } else {
      parts.push(<span key={key++}>{remaining}</span>);
      break;
    }
  }
  return parts;
}

// ═══════════════════════════════════════════════════════════════
// dispatchToolResult — traduit un tool_result en action App.jsx
//
// Gère deux formats :
//   LEGACY (call_llm direct) :
//     tr.type === "FeatureCollection"      → add_layer (vecteur Overture)
//     tr.action === "fly_to" | "remove_layer" | ...
//
//   ORCHESTRATEUR (mcp_client) :
//     tr.action === "add_layer"  + tr.tile_url  → add_raster_layer (GEE/raster)
//     tr.action === "add_layer"  + tr.geojson   → add_layer (vecteur)
//     tr.action === "add_markers"               → add_layer (FeatureCollection)
//     tr.action === "add_choropleth"            → add_layer (world_data)
//     tr.action === "add_isochrone"             → compute_isochrone (GeoJSON prêt)
//     tr.action === "add_route"                 → compute_route (GeoJSON prêt)
//     tr.action === "add_timelapse"             → add_timelapse
//     tr.action === "show_elevation_profile"    → elevation_profile
// ═══════════════════════════════════════════════════════════════
function dispatchToolResult(tr, tc, onToolResult) {
  if (!tr || !onToolResult) return;

  const action = tr.action || "";

  // ── Format LEGACY call_llm ────────────────────────────────
  if (tr.type === "FeatureCollection") {
    onToolResult({ type: "add_layer", data: tr, tool: tc });
    return;
  }

  // ── Format ORCHESTRATEUR : action=add_layer ───────────────
  if (action === "add_layer") {
    // Raster GEE — tile_url XYZ présent
    if (tr.tile_url) {
      const bbox = tr.bbox || tr.clip_bbox || null;
      onToolResult({
        type:       "add_raster_layer",
        tile_url:   tr.tile_url,
        name:       tr.layer_name || tr.name || tc?.name || "GEE layer",
        dataset:    tr.dataset    || "",
        index:      tr.index      || tr.band || "",
        date_start: tr.date_start || tr.date || null,
        date_end:   tr.date_end   || tr.date || null,
        cloud_max:  tr.cloud_max  || 20,
        composite:  tr.composite  || "median",
        bbox,
        clip_bbox:  tr.clip_bbox  || tr.bbox || null,
        vis_params: tr.vis_params || tr.visParams || null,
      });
      // Auto-zoom sur la bbox de la couche GEE
      if (bbox?.length === 4) {
        onToolResult({ type: "fly_bbox", bbox });
      }
      return;
    }
    // GeoJSON vecteur encapsulé dans action=add_layer
    if (tr.geojson) {
      onToolResult({ type: "add_layer", data: tr.geojson, tool: tc });
      return;
    }
    // Fallback : on tente de passer tr directement
    onToolResult({ type: "add_layer", data: tr, tool: tc });
    return;
  }

  // ── Marqueurs / features vecteur ─────────────────────────
  if (action === "add_markers" || action === "add_features") {
    const gj = tr.geojson || tr;
    if (gj?.type === "FeatureCollection") {
      // Enrichir les metadata avec layer_name et clip_to_layer si présents dans tr
      const enriched = {
        ...gj,
        metadata: {
          ...(gj.metadata || {}),
          ...(tr.layer_name     ? { layer_name:    tr.layer_name }    : {}),
          ...(tr.clip_to_layer  ? { clip_to_layer: tr.clip_to_layer } : {}),
          ...(tr._layer_name    ? { layer_name:    tr._layer_name }   : {}),
          ...(tr._clip_to_layer ? { clip_to_layer: tr._clip_to_layer }: {}),
          ...(tr.category       ? { category:      tr.category }      : {}),
        },
      };
      onToolResult({ type: "add_layer", data: enriched, tool: tc });
    }
    return;
  }

  // ── Choroplèthe World Bank ────────────────────────────────
  if (action === "add_choropleth") {
    const gj = tr.geojson || tr;
    if (gj?.type === "FeatureCollection") {
      // Injecter les métadonnées pour que handleToolResult détecte world_data
      const enriched = {
        ...gj,
        metadata: {
          ...(gj.metadata || {}),
          theme:     "world_data",
          indicator: tr.indicator || gj.metadata?.indicator,
          label:     tr.indicator_label || tr.label || gj.metadata?.label,
          unit:      tr.unit || gj.metadata?.unit || "",
          year:      tr.year || gj.metadata?.year,
        },
      };
      onToolResult({ type: "add_layer", data: enriched, tool: tc });
    }
    return;
  }

  // ── Isochrone prête (GeoJSON déjà calculé côté MCP) ───────
  if (action === "add_isochrone") {
    const gj = tr.geojson || tr;
    if (gj?.type === "FeatureCollection") {
      onToolResult({ type: "add_layer", data: gj, tool: tc });
    } else {
      // Laisser App.jsx calculer via computeIsochrone
      onToolResult({ type: "compute_isochrone", center: tr.center, time_minutes: tr.time_minutes || 10, profile: tr.profile || "foot" });
    }
    return;
  }

  // ── Route prête (GeoJSON déjà calculé côté MCP) ───────────
  if (action === "add_route") {
    const gj = tr.geojson || tr;
    if (gj?.type === "FeatureCollection") {
      onToolResult({ type: "add_layer", data: gj, tool: tc });
    } else {
      onToolResult({ type: "compute_route", waypoints: tr.waypoints || [], profile: tr.profile || "foot" });
    }
    return;
  }

  // ── Timelapse GEE ─────────────────────────────────────────
  if (action === "add_timelapse") {
    onToolResult({ type: "add_timelapse", ...tr });
    return;
  }

  // ── Profil altimétrique ───────────────────────────────────
  if (action === "show_elevation_profile") {
    onToolResult({ type: "elevation_profile", ...tr });
    return;
  }

  // ── Actions legacy directes ───────────────────────────────
  if (action === "fly_to")           { onToolResult({ type: "fly_to",          ...tr }); return; }
  if (action === "set_layer_style")  { onToolResult({ type: "set_style",        ...tr }); return; }
  if (action === "remove_layer")     { onToolResult({ type: "remove_layer",     ...tr }); return; }
  if (action === "spatial_analysis") { onToolResult({ type: "spatial_analysis", ...tr }); return; }
  if (action === "thematic_analysis") {
    onToolResult({
      type:        "thematic_analysis",
      layer_name:  tr.layer_name,
      operation:   tr.analysis_type  || tr.operation  || "choropleth",
      attribute:   tr.attribute,
      palette:     tr.color_ramp     || tr.palette     || "viridis",
      method:      tr.method         || "jenks",
      n_classes:   tr.n_classes      || 5,
      min_size:    tr.min_radius     || tr.min_size    || 3,
      max_size:    tr.max_radius     || tr.max_size    || 40,
      geom_type:   tr.geom_type      || null,
      result_name: tr.result_name    || null,
    });
    return;
  }
  if (action === "set_3d_extrusion") { onToolResult({ type: "set_3d_extrusion", ...tr }); return; }
  if (action === "compute_route")    { onToolResult({ type: "compute_route",    ...tr }); return; }
  if (action === "compute_isochrone"){ onToolResult({ type: "compute_isochrone",...tr }); return; }

  // ── Inconnu — log pour debug ──────────────────────────────
  console.warn("[ChatPanel] tool_result non dispatché :", action, tr);
}


export default function ChatPanel({ onToolResult, mapContext, onSendRef }) {
  const C = getTheme();
  const [msgs, setMsgs] = useState(() => {
    try { const saved = localStorage.getItem("ome-chat"); return saved ? JSON.parse(saved) : []; } catch { return []; }
  });
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [config, setConfig]   = useState(null);
  const ref = useRef(null);

  useEffect(() => { fetch(`${API}/config`).then(r => r.json()).then(setConfig).catch(() => {}); }, []);
  useEffect(() => { ref.current?.scrollTo({ top: ref.current.scrollHeight, behavior: "smooth" }); }, [msgs]);
  useEffect(() => { try { localStorage.setItem("ome-chat", JSON.stringify(msgs.slice(-50))); } catch {} }, [msgs]);

  // ── Appel API central ─────────────────────────────────────
  const callAPI = useCallback(async (messagesToSend) => {
    const res = await fetch(`${API}/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        messages:    messagesToSend.map(m => ({ role: m.role, content: m.content })),
        map_context: mapContext,
      }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
  }, [mapContext]);

  // ── Traitement de la réponse API ──────────────────────────
  const handleResponse = useCallback((data) => {
    if (data.tool_results?.length) {
      data.tool_results.forEach((tr, i) => {
        dispatchToolResult(tr, data.tool_calls?.[i], onToolResult);
      });
    }
    // Compter les features (legacy + orchestrateur)
    const fc = data.tool_results?.reduce((s, r) => {
      return s + (r.metadata?.total || r.feature_count || r.geojson?.features?.length || 0);
    }, 0) || 0;

    return {
      role:    "assistant",
      content: data.text || "Fait.",
      tools:   data.tool_calls || [],
      fc,
    };
  }, [onToolResult]);

  // ── send (bouton / Enter) ─────────────────────────────────
  const send = useCallback(async () => {
    if (!input.trim() || loading) return;
    const userMsg  = { role: "user", content: input.trim() };
    const newMsgs  = [...msgs, userMsg];
    setMsgs(newMsgs);
    setInput("");
    setLoading(true);
    try {
      const data    = await callAPI(newMsgs);
      const botMsg  = handleResponse(data);
      setMsgs(prev => [...prev, botMsg]);
    } catch (e) {
      setMsgs(prev => [...prev, { role: "assistant", content: `Erreur: ${e.message}` }]);
    }
    setLoading(false);
  }, [input, msgs, loading, callAPI, handleResponse]);

  // ── sendExternal (voix / API externe) ────────────────────
  const sendExternal = useCallback((text) => {
    if (!text?.trim() || loading) return;
    const userMsg = { role: "user", content: text.trim() };
    setMsgs(prev => {
      const nm = [...prev, userMsg];
      (async () => {
        setLoading(true);
        setInput("");
        try {
          const data   = await callAPI(nm);
          const botMsg = handleResponse(data);
          setMsgs(p => [...p, botMsg]);
        } catch (e) {
          setMsgs(p => [...p, { role: "assistant", content: `Erreur: ${e.message}` }]);
        }
        setLoading(false);
      })();
      return nm;
    });
  }, [loading, callAPI, handleResponse]);

  useEffect(() => {
    if (onSendRef) onSendRef.current = sendExternal;
  }, [sendExternal, onSendRef]);

  // ── Modules de l'application ────────────────────────────
  const MODULES = [
    { icon: IcMap, label: "Overture Maps", desc: "POI, bâtiments, routes, divisions" },
    { icon: IcSatellite, label: "Google Earth Engine", desc: "NDVI, LST, WorldCover, SAR, SRTM…" },
    { icon: IcGlobe, label: "World Bank", desc: "Indicateurs mondiaux par pays" },
    { icon: IcRoute, label: "Routage & Isochrones", desc: "Itinéraires, zones accessibles" },
    { icon: IcBarChart, label: "Analyse thématique", desc: "Choroplèthe, symboles, classification" },
    { icon: IcVenn, label: "Analyse spatiale", desc: "Buffer, clip, intersection, DBSCAN" },
    { icon: IcServer, label: "OSM / OGC / WMS", desc: "OpenStreetMap, services web" },
    { icon: IcTrendingUp, label: "Profil altimétrique", desc: "Coupe topographique IGN 1m" },
  ];

  const PROMPT_EXAMPLES = [
    { icon: IcSatellite, text: "NDVI sur Dakar entre janvier et juin 2024" },
    { icon: IcBuilding, text: "Restaurants autour du Château des Ducs de Bretagne" },
    { icon: IcBarChart, text: "Carte choroplèthe de la population sur la couche Africa_cities" },
  ];

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", background: C.card, borderLeft: `0.5px solid ${C.bdr}` }}>
      {/* Header */}
      <div style={{ padding: "10px 14px", borderBottom: `0.5px solid ${C.bdr}`, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ fontSize: 13, fontWeight: 500 }}>Assistant carto</div>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          {msgs.length > 0 && (
            <button
              onClick={() => { setMsgs([]); try { localStorage.removeItem("ome-chat"); } catch {} }}
              style={{ fontFamily: F, fontSize: 9, padding: "2px 6px", borderRadius: 4, background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.dim, cursor: "pointer" }}
            >Effacer</button>
          )}
          {config && <Badge color={C.acc}>{config.llm_provider}</Badge>}
        </div>
      </div>

      {/* Messages */}
      <div ref={ref} style={{ flex: 1, overflowY: "auto", padding: 12, display: "flex", flexDirection: "column", gap: 10 }}>
        {msgs.length === 0 && (
          <div style={{ fontSize: 12, color: C.txt }}>

            {/* Bienvenue */}
            <div style={{ background: C.acc + "12", border: `0.5px solid ${C.acc}33`, borderRadius: 10, padding: "12px 14px", marginBottom: 10 }}>
              <div style={{ fontSize: 14, fontWeight: 600, color: C.acc, marginBottom: 4, display: "flex", alignItems: "center", gap: 7 }}><IcSparkles size={16}/> Bienvenue sur OpenMapAgents</div>
              <div style={{ fontSize: 11, color: C.dim, lineHeight: 1.6 }}>
                Votre assistant cartographique IA. Décrivez ce que vous voulez voir sur la carte — données satellitaires, vecteurs, analyses, itinéraires — en langage naturel.
              </div>
            </div>

            {/* Modules disponibles */}
            <div style={{ marginBottom: 10 }}>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.mut, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Modules disponibles</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 3 }}>
                {MODULES.map(m => (
                  <div key={m.label} style={{ display: "flex", alignItems: "center", gap: 8, padding: "5px 8px", borderRadius: 6, background: C.hover }}>
                    <span style={{ flexShrink: 0, display: "flex", color: C.acc }}>{m.icon && <m.icon size={15}/>}</span>
                    <div>
                      <div style={{ fontSize: 11, fontWeight: 500, color: C.txt }}>{m.label}</div>
                      <div style={{ fontSize: 9, color: C.dim }}>{m.desc}</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Exemples de prompts */}
            <div>
              <div style={{ fontSize: 10, fontWeight: 600, color: C.mut, textTransform: "uppercase", letterSpacing: 1, marginBottom: 6 }}>Essayez par exemple</div>
              {PROMPT_EXAMPLES.map(p => (
                <button key={p.text} onClick={() => setInput(p.text)} style={{
                  fontFamily: F, fontSize: 11, padding: "8px 10px", borderRadius: 8,
                  background: C.hover, border: `0.5px solid ${C.bdr}`, color: C.txt,
                  cursor: "pointer", textAlign: "left", display: "flex", alignItems: "center",
                  gap: 8, width: "100%", marginBottom: 5,
                }}>
                  <span style={{ flexShrink: 0, display: "flex", color: C.acc }}>{p.icon && <p.icon size={15}/>}</span>
                  <span style={{ lineHeight: 1.4 }}>{p.text}</span>
                </button>
              ))}
            </div>

          </div>
        )}

        {msgs.map((m, i) => (
          <div key={i} style={{
            alignSelf: m.role === "user" ? "flex-end" : "flex-start",
            maxWidth: "88%", padding: "8px 12px", borderRadius: 10,
            background: m.role === "user" ? C.acc + "18" : C.hover,
            border: `0.5px solid ${m.role === "user" ? C.acc + "33" : C.bdr}`,
          }}>
            <MarkdownText text={m.content} color={C.txt} />
            {m.tools?.length > 0 && (
              <div style={{ marginTop: 6, display: "flex", gap: 4, flexWrap: "wrap" }}>
                {m.tools.map((t, j) => <Badge key={j} color={C.blu}>{t.name}</Badge>)}
                {m.fc > 0 && <Badge color={C.amb}>{m.fc} features</Badge>}
              </div>
            )}
          </div>
        ))}

        {loading && (
          <div style={{
            alignSelf: "flex-start", padding: "8px 14px", borderRadius: 10,
            background: C.hover, border: `0.5px solid ${C.bdr}`,
            display: "flex", alignItems: "center", gap: 8,
          }}>
            <Spinner /><span style={{ fontSize: 12, color: C.dim }}>Recherche...</span>
          </div>
        )}
      </div>

      {/* Saisie — désactivable par variable d'environnement (voir CHAT_INPUT_ENABLED).
          Les réponses déjà présentes et les envois programmatiques (sendExternal,
          utilisé par les autres panneaux) restent fonctionnels : seule la saisie
          libre par l'utilisateur est retirée. */}
      {CHAT_INPUT_ENABLED ? (
        <div style={{ padding: "10px 12px", borderTop: `0.5px solid ${C.bdr}`, display: "flex", gap: 6, alignItems: "center" }}>
          <input
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === "Enter" && !e.shiftKey && send()}
            placeholder="NDVI sur Nantes, restaurants à Dakar..."
            style={{
              flex: 1, fontFamily: F, fontSize: 13, padding: "8px 12px", borderRadius: 8,
              background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none",
            }}
          />
          <VoiceInput onResult={(text) => { setInput(text); setTimeout(() => { sendExternal(text); }, 100); }} />
          <button onClick={send} disabled={loading || !input.trim()} style={{
            fontFamily: F, fontSize: 12, fontWeight: 500, padding: "8px 14px", borderRadius: 8,
            background: input.trim() ? C.acc : C.hover, color: input.trim() ? "#fff" : C.dim,
            border: "none", cursor: input.trim() ? "pointer" : "default",
          }}>Envoyer</button>
        </div>
      ) : (
        <div style={{
          padding: "10px 12px", borderTop: `0.5px solid ${C.bdr}`,
          display: "flex", alignItems: "center", gap: 7, fontSize: 11, color: C.dim,
        }}>
          <IcLock size={13} /> Saisie désactivée sur cette instance.
        </div>
      )}
    </div>
  );
}
