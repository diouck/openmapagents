/**
 * StoryPanel.jsx — Story maps : scrollytelling + export HTML autonome.
 *
 * On compose une histoire en chapitres ; chaque chapitre mémorise une VUE
 * (centre/zoom/inclinaison/orientation, capturée depuis la carte) + un texte +
 * les couches visibles. « Lire » enchaîne les vues dans l'appli. « Exporter »
 * produit un fichier .html autonome : au défilement, la carte vole d'un chapitre
 * à l'autre (MapLibre GL JS via CDN + fond OpenStreetMap), couches embarquées.
 */
import { useState, useCallback, useRef, useEffect } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";

const uid = () => Math.random().toString(36).slice(2, 9);
const escapeScript = (s) => JSON.stringify(s).replace(/</g, "\\u003c");
const slugify = (s) => (s || "story").normalize("NFD").replace(/[̀-ͯ]/g, "")
  .toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 60) || "story";

// Couche appli → forme sérialisable pour l'export.
function layerToExport(l) {
  if (l.kind === "image" && l.imageUrl && l.coordinates)
    return { id: l.id, name: l.name, type: "image", url: l.imageUrl, coordinates: l.coordinates, opacity: l.opacity ?? 0.9 };
  if (l.geojson?.features?.length)
    return { id: l.id, name: l.name, type: "geojson", data: l.geojson, color: l.color || "#3b82f6", opacity: l.opacity ?? 1 };
  return null;
}

function buildStoryHTML(story, exportLayers) {
  const data = escapeScript({ chapters: story.chapters, layers: exportLayers });
  const title = story.title || "Story map";
  const sub = story.subtitle || "";
  const first = story.chapters[0]?.camera || { lng: 2.3, lat: 46.6, zoom: 4, pitch: 0, bearing: 0 };
  const sections = story.chapters.map((c, i) => `
    <section class="chap" data-idx="${i}">
      <div class="card">
        <h2>${(c.title || `Chapitre ${i + 1}`).replace(/</g, "&lt;")}</h2>
        <p>${(c.text || "").replace(/</g, "&lt;").replace(/\n/g, "<br>")}</p>
      </div>
    </section>`).join("");
  return `<!DOCTYPE html>
<html lang="fr"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${title.replace(/</g, "&lt;")}</title>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet">
<style>
  :root{--fg:#f4f4f5;--bg:rgba(20,22,28,.86);}
  *{box-sizing:border-box} html,body{margin:0;height:100%;font-family:system-ui,-apple-system,"Segoe UI",sans-serif}
  #map{position:fixed;inset:0}
  #story{position:relative;z-index:2;width:min(420px,92vw);margin:0;padding:0 24px}
  section.title,section.chap,section.end{min-height:92vh;display:flex;align-items:center}
  .card{background:var(--bg);color:var(--fg);border-radius:14px;padding:22px 24px;box-shadow:0 8px 40px rgba(0,0,0,.4);backdrop-filter:blur(6px)}
  section.title .card{background:rgba(20,22,28,.92)}
  h1{margin:0 0 8px;font-size:26px;line-height:1.2} h2{margin:0 0 10px;font-size:19px}
  p{margin:0;font-size:15px;line-height:1.6;color:#d7d7db} .sub{color:#a9a9b2;font-size:15px}
  .credit{position:fixed;bottom:6px;right:8px;z-index:3;font-size:11px;color:#333;background:rgba(255,255,255,.7);padding:2px 6px;border-radius:4px}
  a{color:#8ab4ff}
</style></head><body>
<div id="map"></div>
<div id="story">
  <section class="title"><div class="card"><h1>${title.replace(/</g, "&lt;")}</h1>${sub ? `<p class="sub">${sub.replace(/</g, "&lt;")}</p>` : ""}</div></section>
  ${sections}
  <section class="end"><div class="card"><p>Fin.</p></div></section>
</div>
<div class="credit">Fond © OpenStreetMap · OpenMapAgents</div>
<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
<script>
const STORY = ${data};
const basemap = {version:8,sources:{osm:{type:"raster",tiles:["https://a.tile.openstreetmap.org/{z}/{x}/{y}.png","https://b.tile.openstreetmap.org/{z}/{x}/{y}.png"],tileSize:256,attribution:"© OpenStreetMap"}},layers:[{id:"osm",type:"raster",source:"osm"}]};
const c0=${escapeScript(first)};
const map=new maplibregl.Map({container:"map",style:basemap,center:[c0.lng,c0.lat],zoom:c0.zoom,pitch:c0.pitch||0,bearing:c0.bearing||0});
map.addControl(new maplibregl.NavigationControl({visualizePitch:true}),"top-right");
function addLayers(){
  STORY.layers.forEach(function(l){
    if(map.getSource(l.id))return;
    if(l.type==="image"){
      map.addSource(l.id,{type:"image",url:l.url,coordinates:l.coordinates});
      map.addLayer({id:l.id+"__r",type:"raster",source:l.id,layout:{visibility:"none"},paint:{"raster-opacity":l.opacity!=null?l.opacity:0.9}});
    }else if(l.type==="geojson"){
      map.addSource(l.id,{type:"geojson",data:l.data});
      map.addLayer({id:l.id+"__fill",type:"fill",source:l.id,filter:["==",["geometry-type"],"Polygon"],layout:{visibility:"none"},paint:{"fill-color":l.color,"fill-opacity":0.35}});
      map.addLayer({id:l.id+"__line",type:"line",source:l.id,filter:["any",["==",["geometry-type"],"LineString"],["==",["geometry-type"],"Polygon"]],layout:{visibility:"none"},paint:{"line-color":l.color,"line-width":2}});
      map.addLayer({id:l.id+"__pt",type:"circle",source:l.id,filter:["==",["geometry-type"],"Point"],layout:{visibility:"none"},paint:{"circle-radius":5,"circle-color":l.color,"circle-stroke-width":1.5,"circle-stroke-color":"#fff"}});
    }
  });
}
function setVisible(ids){
  var on={}; (ids||[]).forEach(function(i){on[i]=1});
  STORY.layers.forEach(function(l){
    var v=on[l.id]?"visible":"none";
    ["__r","__fill","__line","__pt"].forEach(function(sfx){ if(map.getLayer(l.id+sfx)) map.setLayoutProperty(l.id+sfx,"visibility",v); });
  });
}
function go(i){ var c=STORY.chapters[i]; if(!c)return; map.flyTo({center:[c.camera.lng,c.camera.lat],zoom:c.camera.zoom,pitch:c.camera.pitch||0,bearing:c.camera.bearing||0,duration:1600,essential:true}); setVisible(c.layers); }
map.on("load",function(){
  addLayers();
  var io=new IntersectionObserver(function(es){
    es.forEach(function(e){ if(e.isIntersecting){ var i=+e.target.getAttribute("data-idx"); go(i); } });
  },{threshold:0.6});
  document.querySelectorAll("section.chap").forEach(function(s){io.observe(s)});
});
</script></body></html>`;
}

export default function StoryPanel({ mapRef, layers = [] }) {
  const C = useThemeContext();
  const [title, setTitle] = useState("Mon histoire");
  const [subtitle, setSubtitle] = useState("");
  const [chapters, setChapters] = useState([]);
  const [active, setActive] = useState(null);
  const [msg, setMsg] = useState(null);
  const playRef = useRef(null);

  useEffect(() => () => { if (playRef.current) clearInterval(playRef.current); }, []);

  const getCamera = () => {
    const m = mapRef?.current?.getMap?.();
    if (!m) return null;
    const c = m.getCenter();
    return { lng: +c.lng.toFixed(6), lat: +c.lat.toFixed(6), zoom: +m.getZoom().toFixed(2), pitch: +m.getPitch().toFixed(1), bearing: +m.getBearing().toFixed(1) };
  };
  const visibleIds = () => layers.filter((l) => l.visible !== false).map((l) => l.id);

  const addChapter = () => {
    const cam = getCamera();
    if (!cam) { setMsg("Carte indisponible."); return; }
    setChapters((cs) => [...cs, { id: uid(), title: `Chapitre ${cs.length + 1}`, text: "", camera: cam, layers: visibleIds() }]);
  };
  const patch = (id, up) => setChapters((cs) => cs.map((c) => (c.id === id ? { ...c, ...up } : c)));
  const recapture = (id) => { const cam = getCamera(); if (cam) patch(id, { camera: cam, layers: visibleIds() }); };
  const del = (id) => setChapters((cs) => cs.filter((c) => c.id !== id));
  const move = (id, d) => setChapters((cs) => {
    const i = cs.findIndex((c) => c.id === id); const j = i + d;
    if (i < 0 || j < 0 || j >= cs.length) return cs;
    const n = cs.slice(); [n[i], n[j]] = [n[j], n[i]]; return n;
  });
  const flyTo = (c) => {
    const m = mapRef?.current?.getMap?.(); if (!m || !c) return;
    m.flyTo({ center: [c.camera.lng, c.camera.lat], zoom: c.camera.zoom, pitch: c.camera.pitch, bearing: c.camera.bearing, duration: 1500, essential: true });
    setActive(c.id);
  };

  const play = () => {
    if (playRef.current) { clearInterval(playRef.current); playRef.current = null; setMsg(null); return; }
    if (!chapters.length) return;
    let i = 0; flyTo(chapters[0]); setMsg("Lecture…");
    playRef.current = setInterval(() => {
      i++;
      if (i >= chapters.length) { clearInterval(playRef.current); playRef.current = null; setMsg(null); return; }
      flyTo(chapters[i]);
    }, 3200);
  };

  const exportHTML = useCallback(() => {
    if (!chapters.length) { setMsg("Ajoutez au moins un chapitre."); return; }
    const used = new Set(); chapters.forEach((c) => (c.layers || []).forEach((id) => used.add(id)));
    const exportLayers = layers.filter((l) => used.has(l.id)).map(layerToExport).filter(Boolean);
    const html = buildStoryHTML({ title, subtitle, chapters }, exportLayers);
    const blob = new Blob([html], { type: "text/html;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a"); a.href = url; a.download = `${slugify(title)}.html`;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 2000);
    setMsg(`Export : ${chapters.length} chapitre(s), ${exportLayers.length} couche(s) embarquée(s).`);
  }, [chapters, title, subtitle, layers]);

  const lbl = { fontSize: 10, fontWeight: 500, color: C.dim, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 2 };
  const inp = { fontFamily: F, fontSize: 12, padding: "6px 8px", borderRadius: 7, border: `0.5px solid ${C.bdr}`, background: C.input || C.bg2 || C.bg, color: C.txt, outline: "none", width: "100%", boxSizing: "border-box" };
  const mini = (bg, col) => ({ fontFamily: F, fontSize: 10.5, padding: "3px 7px", borderRadius: 5, border: `0.5px solid ${col}55`, background: bg, color: col, cursor: "pointer" });

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, height: "100%", minHeight: 0, padding: 12, boxSizing: "border-box" }}>
      <div>
        <div style={lbl}>Titre de l'histoire</div>
        <input value={title} onChange={(e) => setTitle(e.target.value)} style={inp} />
      </div>
      <div>
        <div style={lbl}>Sous-titre</div>
        <input value={subtitle} onChange={(e) => setSubtitle(e.target.value)} placeholder="(optionnel)" style={inp} />
      </div>

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        <button onClick={addChapter} style={{ fontFamily: F, fontSize: 12, fontWeight: 600, padding: "7px 12px", cursor: "pointer", background: C.acc, color: "#fff", border: "none", borderRadius: 7 }}>
          + Chapitre (vue actuelle)
        </button>
        <button onClick={play} disabled={!chapters.length}
          style={{ fontFamily: F, fontSize: 12, fontWeight: 500, padding: "7px 12px", cursor: chapters.length ? "pointer" : "not-allowed", background: "transparent", color: C.acc, border: `1px solid ${C.acc}66`, borderRadius: 7 }}>
          {playRef.current ? "⏸ Stop" : "▶ Lire"}
        </button>
        <button onClick={exportHTML} disabled={!chapters.length}
          style={{ fontFamily: F, fontSize: 12, fontWeight: 500, padding: "7px 12px", cursor: chapters.length ? "pointer" : "not-allowed", background: "transparent", color: C.txt, border: `1px solid ${C.bdr}`, borderRadius: 7, marginLeft: "auto" }}>
          ⬇ Exporter en HTML
        </button>
      </div>

      {msg && <div style={{ fontFamily: F, fontSize: 11.5, color: C.acc, background: C.acc + "12", border: `0.5px solid ${C.acc}44`, borderRadius: 6, padding: "6px 10px" }}>{msg}</div>}

      <div style={{ flex: 1, minHeight: 0, overflowY: "auto", display: "flex", flexDirection: "column", gap: 8 }}>
        {chapters.length === 0 && (
          <div style={{ fontFamily: F, fontSize: 11.5, color: C.dim, lineHeight: 1.5 }}>
            Cadrez la carte, ajoutez un chapitre : sa vue (centre, zoom, inclinaison) et les couches visibles sont mémorisées. Enchaînez les chapitres, puis « Lire » ou « Exporter en HTML » (fichier autonome à faire défiler).
          </div>
        )}
        {chapters.map((c, i) => (
          <div key={c.id} style={{ border: `0.5px solid ${active === c.id ? C.acc : C.bdr}`, borderRadius: 8, padding: 8, background: C.bg2 || C.bg, display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <span style={{ fontFamily: M, fontSize: 10.5, color: C.dim }}>{i + 1}</span>
              <input value={c.title} onChange={(e) => patch(c.id, { title: e.target.value })} style={{ ...inp, padding: "4px 7px", fontWeight: 600 }} />
              <button onClick={() => move(c.id, -1)} title="Monter" style={mini("transparent", C.mut)}>↑</button>
              <button onClick={() => move(c.id, 1)} title="Descendre" style={mini("transparent", C.mut)}>↓</button>
              <button onClick={() => del(c.id)} title="Supprimer" style={mini("transparent", "#e11d1d")}>✕</button>
            </div>
            <textarea value={c.text} onChange={(e) => patch(c.id, { text: e.target.value })} placeholder="Texte du chapitre…"
              style={{ ...inp, minHeight: 52, resize: "vertical", fontSize: 12 }} />
            <div style={{ display: "flex", alignItems: "center", gap: 6, flexWrap: "wrap" }}>
              <span style={{ fontFamily: M, fontSize: 10, color: C.dim }}>
                z{c.camera.zoom} · {c.camera.lat}, {c.camera.lng}{c.camera.pitch ? ` · ${c.camera.pitch}°` : ""} · {(c.layers || []).length} couche(s)
              </span>
              <button onClick={() => flyTo(c)} style={{ ...mini(C.acc + "12", C.acc), marginLeft: "auto" }}>Aller</button>
              <button onClick={() => recapture(c.id)} title="Remplacer par la vue actuelle" style={mini("transparent", C.mut)}>Recapturer</button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
