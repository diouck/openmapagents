/**
 * LidarPanel.jsx — Menu LiDAR : import d'un nuage LAS/LAZ puis foresterie
 * (MNT / MNS / MNH, détection d'arbres, houppiers).
 *
 * Le fichier est envoyé une seule fois (/api/lidar/points, qui le met en cache
 * et renvoie un jeton) ; la foresterie (CanopyPanel) réutilise ce jeton sans
 * ré-uploader. Les produits sont ajoutés automatiquement au menu Couches.
 */
import { useState, useRef, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import CanopyPanel from "./CanopyPanel";
import { registerPC, applyPCStyle } from "../utils/lidarStyle";
import { IcTreePine, IcUpload } from "../icons";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

// base64 → ArrayBuffer
function _b64ToBuf(b64) {
  const bin = atob(b64); const n = bin.length; const b = new Uint8Array(n);
  for (let i = 0; i < n; i++) b[i] = bin.charCodeAt(i);
  return b.buffer;
}

function uploadPoints(file, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${API}/api/lidar/points`);
    xhr.upload.onprogress = (e) => { if (e.lengthComputable) onProgress(e.loaded / e.total); };
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try { resolve(JSON.parse(xhr.responseText)); } catch (_) { reject(new Error("Réponse serveur illisible.")); }
      } else if (xhr.status === 413) {
        reject(new Error("Fichier trop volumineux (limite d'upload nginx « client_max_body_size »)."));
      } else {
        let m = `Erreur ${xhr.status}`;
        try { m = JSON.parse(xhr.responseText).detail || m; } catch (_) {}
        reject(new Error(m));
      }
    };
    xhr.onerror = () => reject(new Error("Échec réseau pendant l'envoi."));
    const fd = new FormData(); fd.append("file", file); xhr.send(fd);
  });
}

export default function LidarPanel({ mapRef, onAddLayer, onAddImageLayer, onAddPointcloudLayer }) {
  const C = useThemeContext();
  const fileRef = useRef(null);
  const [file, setFile] = useState(null);
  const [token, setToken] = useState(null);
  const [info, setInfo] = useState(null);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState(null);

  const getMap = useCallback(() => mapRef.current?.getMap?.() || null, [mapRef]);

  const onPick = async (e) => {
    const f = e.target.files?.[0]; e.target.value = "";
    if (!f) return;
    const ext = (f.name.split(".").pop() || "").toLowerCase();
    if (!["las", "laz"].includes(ext)) { setStatus({ t: "error", m: "Formats acceptés : .las, .laz" }); return; }
    setFile(f); setToken(null); setInfo(null);
    try {
      setBusy(true);
      const mb = (f.size / 1048576).toFixed(0);
      const d = await uploadPoints(f, (p) => setStatus({
        t: "info", m: p < 1 ? `Envoi… ${Math.round(p * 100)}% (${mb} Mo)` : "Lecture du nuage sur le serveur…",
      }));
      setToken(d.file_token || null);
      setInfo({ crs: d.crs, count: d.count, total: d.total, version: d.version, hist: d.class_histogram || {}, center: d.center_lonlat });

      // ── Affichage du nuage de points (deck.gl) + entrée menu Couches ──
      const map = getMap();
      if (map && d.center_lonlat && d.positions_b64) {
        try {
          const positions = new Float32Array(_b64ToBuf(d.positions_b64));
          const classification = d.classification_b64 ? new Uint8Array(_b64ToBuf(d.classification_b64)) : null;
          const rgb = d.colors_b64 ? new Uint8Array(_b64ToBuf(d.colors_b64)) : null;
          const pcId = "lidar_pc_" + Date.now();
          registerPC(pcId, {
            full: { positions, rgb, classification, count: d.count },
            histogram: d.class_histogram || {},
            anchor: [d.center_lonlat[0], d.center_lonlat[1], 0],
            name: `Nuage ${f.name}`,
          });
          await applyPCStyle(map, pcId, {});   // coloration par défaut (classif > RGB > élévation)
          try { map.flyTo({ center: [d.center_lonlat[0], d.center_lonlat[1]], zoom: Math.max(map.getZoom(), 15), pitch: 50, duration: 1200 }); } catch (_) {}
          onAddPointcloudLayer?.({ id: pcId, name: `Nuage ${f.name}`, count: d.count });
        } catch (err) { console.warn("Affichage nuage:", err); }
      } else if (d.center_lonlat) {
        try { map?.flyTo({ center: [d.center_lonlat[0], d.center_lonlat[1]], zoom: 15, duration: 1200 }); } catch (_) {}
      }
      setStatus({ t: "ok", m: `${f.name} affiché (${(d.total || d.count).toLocaleString()} pts · ${d.crs || "CRS ?"})` });
    } catch (err) {
      setStatus({ t: "error", m: err?.message || String(err) }); setFile(null);
    }
    setBusy(false);
  };

  const statColor = { ok: C.acc, error: C.red, info: C.amb };

  return (
    <div style={{ flex: 1, overflowY: "auto", padding: 10, display: "flex", flexDirection: "column", gap: 12 }}>
      <div style={{ background: C.hover, borderRadius: 8, padding: "10px 12px", border: `0.5px solid ${C.bdr}` }}>
        <div style={{ fontSize: 12, fontWeight: 600, color: C.txt, display: "flex", alignItems: "center", gap: 8 }}>
          <IcTreePine size={17} color={C.acc}/> LiDAR — Foresterie
        </div>
        <div style={{ fontSize: 9, color: C.dim, marginTop: 4, lineHeight: 1.5 }}>
          Importez un nuage LAS/LAZ, puis générez le <b>MNT</b>, le <b>MNS</b>, le <b>MNH</b> (canopée),
          et détectez arbres & houppiers. Les produits s'ajoutent au menu <b>Couches</b>.
        </div>
      </div>

      <button onClick={() => fileRef.current?.click()} disabled={busy} style={{
        fontFamily: F, fontSize: 12, padding: "9px 0", borderRadius: 6, width: "100%",
        background: "transparent", border: `1px dashed ${C.acc}77`, color: C.acc, cursor: busy ? "default" : "pointer",
        display: "flex", alignItems: "center", justifyContent: "center", gap: 7,
      }}><IcUpload size={14}/> {busy ? "Chargement…" : "Importer un nuage LiDAR (.las .laz)"}</button>
      <input ref={fileRef} type="file" accept=".las,.laz" onChange={onPick} style={{ display: "none" }} />

      {status && (
        <div style={{ fontSize: 10, padding: "5px 8px", borderRadius: 5, lineHeight: 1.5,
          background: statColor[status.t] + "15", border: `0.5px solid ${statColor[status.t]}44`, color: statColor[status.t] }}>
          {status.m}
        </div>
      )}

      {info && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 4, fontSize: 9 }}>
          {[
            ["Points", (info.total || info.count)?.toLocaleString?.() || info.count],
            ["CRS", info.crs || "?"],
            ["Version", "LAS " + info.version],
            ["Classes", Object.keys(info.hist || {}).length],
          ].map(([k, v]) => (
            <div key={k} style={{ background: C.input, borderRadius: 4, padding: "3px 6px", display: "flex", justifyContent: "space-between", gap: 6 }}>
              <span style={{ color: C.dim }}>{k}</span>
              <span style={{ color: C.txt, fontFamily: M, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{v}</span>
            </div>
          ))}
        </div>
      )}

      {(file || token) && (
        <CanopyPanel mapRef={mapRef} file={file} fileToken={token} onAddLayer={onAddLayer} onAddImageLayer={onAddImageLayer} />
      )}
    </div>
  );
}
