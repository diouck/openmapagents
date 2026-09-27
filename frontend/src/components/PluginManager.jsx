/**
 * PluginManager.jsx — Modal « Plugins » (marketplace en cartes).
 * =============================================================================
 * Recherche + filtres (Tous / Installés / Disponibles) ; chaque plugin est une
 * carte (icône, nom, version, description, auteur, tags) avec Installer /
 * (Dés)activer / Désinstaller. Un plugin installé & actif apparaît aussitôt
 * dans le menu thématique (via pluginState).
 * =============================================================================
 */
import { useState, useMemo } from "react";
import {
  PLUGIN_CATALOG, CORE_IDS, usePluginState,
  isInstalled, isDisabled, install, uninstall, setDisabled,
} from "../plugins/pluginState";
import { IcSearch, IcX, IcTrash, IcEye, IcEyeOff, IcCheck } from "../icons";

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

  const counts = {
    all: PLUGIN_CATALOG.length,
    installed: PLUGIN_CATALOG.filter((p) => CORE_IDS.has(p.id) || isInstalled(p.id)).length,
    available: PLUGIN_CATALOG.filter((p) => !(CORE_IDS.has(p.id) || isInstalled(p.id))).length,
  };
  const chip = (k, l) => (
    <button key={k} onClick={() => setFilter(k)} style={{
      fontSize: 11.5, padding: "5px 12px", borderRadius: 999, cursor: "pointer",
      border: `0.5px solid ${filter === k ? C.acc : C.bdr}`,
      background: filter === k ? C.acc : "transparent",
      color: filter === k ? "#04120a" : C.mut, fontWeight: filter === k ? 600 : 400,
    }}>{l} <span style={{ opacity: 0.7 }}>{counts[k]}</span></button>
  );
  const iconBtn = { background: "transparent", border: `0.5px solid ${C.bdr}`, borderRadius: 7, color: C.mut, cursor: "pointer", padding: "5px 6px", display: "flex", alignItems: "center" };

  return (
    <div onClick={onClose} style={{ position: "fixed", inset: 0, zIndex: 2000, background: "rgba(0,0,0,.5)", display: "flex", alignItems: "center", justifyContent: "center", padding: 16 }}>
      <div onClick={(e) => e.stopPropagation()} style={{ width: 520, maxWidth: "100%", maxHeight: "84vh", display: "flex", flexDirection: "column", background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 16, overflow: "hidden" }}>

        <div style={{ display: "flex", alignItems: "center", gap: 9, padding: "13px 16px", borderBottom: `0.5px solid ${C.bdr}` }}>
          <span style={{ fontSize: 15, fontWeight: 600, color: C.txt, flex: 1 }}>Plugins</span>
          <button onClick={onClose} title="Fermer" style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={17} /></button>
        </div>

        <div style={{ padding: "13px 16px", borderBottom: `0.5px solid ${C.bdr}` }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 10, padding: "8px 11px" }}>
            <IcSearch size={15} color={C.dim} />
            <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Rechercher un plugin…" autoFocus
              style={{ flex: 1, minWidth: 0, background: "transparent", border: "none", outline: "none", color: C.txt, fontSize: 13 }} />
            {q && <button onClick={() => setQ("")} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", display: "flex" }}><IcX size={14} /></button>}
          </div>
          <div style={{ display: "flex", gap: 7, marginTop: 10 }}>
            {chip("all", "Tous")}{chip("installed", "Installés")}{chip("available", "Disponibles")}
          </div>
        </div>

        <div style={{ overflowY: "auto", padding: 14, display: "flex", flexDirection: "column", gap: 10 }}>
          {list.length === 0 && <div style={{ padding: "26px 0", textAlign: "center", color: C.dim, fontSize: 12.5 }}>Aucun plugin pour ce filtre.</div>}
          {list.map((p) => {
            const core = CORE_IDS.has(p.id);
            const inst = core || isInstalled(p.id);
            const off = isDisabled(p.id);
            const active = inst && !off;
            const Icon = p.icon;
            return (
              <div key={p.id} style={{ background: C.input, border: `0.5px solid ${C.bdr}`, borderRadius: 12, padding: "12px 13px" }}>
                <div style={{ display: "flex", alignItems: "flex-start", gap: 12 }}>
                  <span style={{ width: 36, height: 36, borderRadius: 10, flexShrink: 0, display: "flex", alignItems: "center", justifyContent: "center",
                                 background: active ? C.acc + "1e" : C.card, color: active ? C.acc : C.mut }}>{Icon && <Icon size={19} />}</span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: "flex", alignItems: "baseline", gap: 7 }}>
                      <span style={{ fontSize: 13.5, fontWeight: 600, color: C.txt, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.label}</span>
                      <span style={{ fontSize: 10, color: C.dim, flexShrink: 0 }}>v{p.version}</span>
                      {active && <IcCheck size={12} color={C.acc} />}
                      {off && <span style={{ fontSize: 9.5, color: C.dim, border: `0.5px solid ${C.bdr}`, borderRadius: 4, padding: "0 5px" }}>désactivé</span>}
                    </div>
                    <div style={{ fontSize: 11.5, color: C.dim, marginTop: 3, lineHeight: 1.4,
                                  display: "-webkit-box", WebkitLineClamp: 2, WebkitBoxOrient: "vertical", overflow: "hidden" }}>{p.desc || p.category}</div>
                  </div>
                  <div style={{ flexShrink: 0, display: "flex", alignItems: "center", gap: 5 }}>
                    {core ? (
                      <span style={{ fontSize: 10.5, color: C.dim }}>cœur</span>
                    ) : !inst ? (
                      <button onClick={() => install(p.id)} style={{ fontSize: 12, fontWeight: 600, color: "#04120a", background: C.acc, border: "none", borderRadius: 8, padding: "6px 14px", cursor: "pointer" }}>Installer</button>
                    ) : (
                      <>
                        <button onClick={() => setDisabled(p.id, !off)} title={off ? "Activer" : "Désactiver"} style={iconBtn}>{off ? <IcEyeOff size={15} /> : <IcEye size={15} />}</button>
                        <button onClick={() => uninstall(p.id)} title="Désinstaller" style={{ ...iconBtn, color: "#f0a8a8" }}><IcTrash size={15} /></button>
                      </>
                    )}
                  </div>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 7, marginTop: 10, flexWrap: "wrap" }}>
                  <span style={{ fontSize: 10.5, color: C.dim }}>par {p.author}</span>
                  <span style={{ width: 3, height: 3, borderRadius: "50%", background: C.dim, opacity: 0.6 }} />
                  {p.tags.map((t) => (
                    <span key={t} style={{ fontSize: 10, color: C.mut, background: C.card, border: `0.5px solid ${C.bdr}`, borderRadius: 6, padding: "2px 8px" }}>{t}</span>
                  ))}
                </div>
              </div>
            );
          })}
        </div>

      </div>
    </div>
  );
}
