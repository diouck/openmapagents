/**
 * PluginManager.jsx — BRIQUE 3 : modal « Plugins ».
 * =============================================================================
 * Recherche + filtres (Tous / Installés / Disponibles) + Installer / Désinstaller
 * / (Dés)activer, sur le catalogue dérivé de MENU_TREE (pluginState).
 * Un plugin installé & actif apparaît aussitôt dans le menu thématique.
 * =============================================================================
 */
import { useState, useMemo } from "react";
import {
  PLUGIN_CATALOG, CORE_IDS, usePluginState,
  isInstalled, isDisabled, install, uninstall, setDisabled,
} from "../plugins/pluginState";
import { IcSearch, IcX } from "../icons";

export default function PluginManager({ open, onClose, C }) {
  const pstate = usePluginState();
  const [q, setQ] = useState("");
  const [filter, setFilter] = useState("all");   // all | installed | available

  const list = useMemo(() => {
    const needle = q.trim().toLowerCase();
    return PLUGIN_CATALOG.filter((p) => {
      if (needle && !`${p.label} ${p.desc} ${p.category}`.toLowerCase().includes(needle)) return false;
      const inst = CORE_IDS.has(p.id) || isInstalled(p.id);
      if (filter === "installed") return inst;
      if (filter === "available") return !inst;
      return true;
    });
  }, [q, filter, pstate]);

  if (!open) return null;

  const chip = (k, l) => (
    <button key={k} onClick={() => setFilter(k)} style={{
      fontSize: 11.5, padding: "5px 11px", borderRadius: 7, cursor: "pointer",
      border: `0.5px solid ${filter === k ? C.acc : C.bdr}`,
      background: filter === k ? C.acc : "transparent",
      color: filter === k ? "#04120a" : C.mut, fontWeight: filter === k ? 600 : 400,
    }}>{l}</button>
  );

  return (
    <div onClick={onClose} style={{ position: "fixed", inset: 0, zIndex: 2000, background: "rgba(0,0,0,.5)", display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }}>
      <div onClick={(e) => e.stopPropagation()} style={{ width: 470, maxWidth: "100%", maxHeight: "82vh", display: "flex", flexDirection: "column", background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 14, overflow: "hidden" }}>

        <div style={{ display: "flex", alignItems: "center", gap: 9, padding: "12px 14px", borderBottom: `0.5px solid ${C.bdr}` }}>
          <span style={{ fontSize: 14, fontWeight: 600, color: C.txt, flex: 1 }}>Plugins</span>
          <button onClick={onClose} title="Fermer" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={16} /></button>
        </div>

        <div style={{ padding: "12px 14px", borderBottom: `0.5px solid ${C.bdr}` }}>
          <div style={{ display: "flex", alignItems: "center", gap: 7, background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 9, padding: "7px 10px" }}>
            <IcSearch size={14} color={C.dim} />
            <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Rechercher un plugin…" autoFocus
              style={{ flex: 1, minWidth: 0, background: "transparent", border: "none", outline: "none", color: C.txt, fontSize: 12.5 }} />
            {q && <button onClick={() => setQ("")} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={13} /></button>}
          </div>
          <div style={{ display: "flex", gap: 6, marginTop: 9 }}>
            {chip("all", "Tous")}{chip("installed", "Installés")}{chip("available", "Disponibles")}
          </div>
        </div>

        <div style={{ overflowY: "auto", padding: "2px 14px 12px" }}>
          {list.length === 0 && <div style={{ padding: "22px 0", textAlign: "center", color: C.dim, fontSize: 12 }}>Aucun plugin pour ce filtre.</div>}
          {list.map((p) => {
            const core = CORE_IDS.has(p.id);
            const inst = core || isInstalled(p.id);
            const off = isDisabled(p.id);
            const Icon = p.icon;
            return (
              <div key={p.id} style={{ display: "flex", alignItems: "center", gap: 11, padding: "10px 2px", borderTop: `0.5px solid ${C.bdr}` }}>
                <span style={{ width: 20, display: "flex", justifyContent: "center", flexShrink: 0, color: inst && !off ? C.acc : C.mut }}>{Icon && <Icon size={18} />}</span>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontSize: 13, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.label}</div>
                  <div style={{ fontSize: 11, color: C.dim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.category}{p.desc ? " · " + p.desc : ""}</div>
                </div>
                {core ? (
                  <span style={{ fontSize: 11, color: C.dim, flexShrink: 0 }}>cœur</span>
                ) : !inst ? (
                  <button onClick={() => install(p.id)} style={{ flexShrink: 0, fontSize: 11.5, fontWeight: 600, color: "#04120a", background: C.acc, border: "none", borderRadius: 7, padding: "5px 13px", cursor: "pointer" }}>Installer</button>
                ) : (
                  <div style={{ display: "flex", gap: 6, flexShrink: 0 }}>
                    <button onClick={() => setDisabled(p.id, !off)} style={{ fontSize: 11.5, color: off ? C.acc : C.mut, background: "transparent", border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: "5px 11px", cursor: "pointer" }}>{off ? "Activer" : "Désactiver"}</button>
                    <button onClick={() => uninstall(p.id)} style={{ fontSize: 11.5, color: "#f0a8a8", background: "transparent", border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: "5px 11px", cursor: "pointer" }}>Désinstaller</button>
                  </div>
                )}
              </div>
            );
          })}
        </div>

      </div>
    </div>
  );
}
