/**
 * ChartStyleBlock.jsx — Bloc dépliable « Graphiques » du panneau Classification.
 *
 * Pose un camembert, des barres ou un treemap sur chaque entité, composé à
 * partir de plusieurs colonnes numériques. Replié par défaut : la classification
 * par couleur reste le réglage courant, les graphiques sont un mode à part.
 *
 * Chaque variable a une pastille de couleur cliquable — la palette donne le
 * point de départ, l'inversion et les remplacements manuels permettent de s'en
 * écarter sans changer de palette.
 */
import { useState, useMemo, useEffect } from "react";
import { useThemeContext } from "../theme";
import { F, M, RAMPS } from "../config";
import { IcChevronDown, IcChevronRight, IcBarChart, IcAlert } from "../icons";
import { CHART_KINDS, resolveChartColors } from "../utils/chartSprites";

export default function ChartStyleBlock({ layer, cfg, onChange, mapRef, layerOpacity = 1, onLayerOpacity }) {
  const C = useThemeContext();
  const [open, setOpen] = useState(!!cfg);
  const [zoom, setZoom] = useState(() => mapRef?.current?.getMap?.()?.getZoom?.() ?? null);
  const [kind, setKind] = useState(cfg?.kind || "pie");
  const [vars, setVars] = useState(cfg?.vars || []);
  const [palette, setPalette] = useState(cfg?.palette || "categorial");
  const [invert, setInvert] = useState(!!cfg?.invert);
  const [colors, setColors] = useState(cfg?.colors || []);
  const [size, setSize] = useState(cfg?.size || "proportional");
  const [minzoom, setMinzoom] = useState(cfg?.minzoom ?? 5);
  const [overlap, setOverlap] = useState(!!cfg?.overlap);
  const [scale,   setScale]   = useState(cfg?.scale ?? 1);
  const [opacity, setOpacity] = useState(cfg?.opacity ?? 1);

  // Colonnes numériques renseignées sur au moins la moitié des entités :
  // écarte les champs anecdotiques qui pollueraient la liste.
  const numAttrs = useMemo(() => {
    const feats = layer?.geojson?.features || [];
    const counts = new Map();
    for (const f of feats.slice(0, 300)) {
      for (const [k, v] of Object.entries(f.properties || {})) {
        const n = typeof v === "number" ? v : Number(v);
        if (Number.isFinite(n)) counts.set(k, (counts.get(k) || 0) + 1);
      }
    }
    const seen = Math.min(feats.length, 300) || 1;
    return [...counts.entries()].filter(([, c]) => c >= seen * 0.5).map(([k]) => k);
  }, [layer]);

  // Suit le zoom courant : c'est la cause n°1 d'un graphique appliqué mais
  // invisible, et rien à l'écran ne l'expliquait.
  useEffect(() => {
    const m = mapRef?.current?.getMap?.(); if (!m || !open) return;
    const upd = () => setZoom(m.getZoom());
    upd(); m.on("zoomend", upd);
    return () => m.off("zoomend", upd);
  }, [mapRef, open]);

  const eff = resolveChartColors({ palette, invert, colors }, RAMPS, Math.max(vars.length, 1));
  const enough = vars.length >= (kind === "bars" ? 1 : 2);

  // Entités réellement porteuses : total > 0 sur les variables choisies.
  // Distingue « masqué par le zoom » de « aucune donnée exploitable ».
  const eligible = useMemo(() => {
    if (!vars.length) return 0;
    let n = 0;
    for (const f of layer?.geojson?.features || []) {
      let t = 0;
      for (const v of vars) { const x = Number(f?.properties?.[v]); if (Number.isFinite(x) && x > 0) t += x; }
      if (t > 0) n++;
    }
    return n;
  }, [layer, vars]);

  const hiddenByZoom = zoom !== null && zoom < minzoom;

  const setColorAt = (i, hex) => {
    const next = [...colors];
    while (next.length < vars.length) next.push(null);
    next[i] = hex;
    setColors(next);
    if (cfg) push({ colors: next });          // déjà appliqué → répercute en direct
  };

  const push = (over = {}) => onChange({
    kind, vars, palette, invert, colors, size, scale, opacity, minzoom, overlap, ...over,
  });

  const btn = (on) => ({
    fontFamily: F, fontSize: 10, padding: "4px 8px", borderRadius: 5, cursor: "pointer",
    background: on ? C.acc + "18" : "transparent",
    border: `0.5px solid ${on ? C.acc + "66" : C.bdr}`, color: on ? C.acc : C.dim,
  });

  return (
    <div style={{ borderTop: `0.5px solid ${C.bdr}`, paddingTop: 8, marginTop: 2 }}>
      <button onClick={() => setOpen(o => !o)} style={{
        width: "100%", display: "flex", alignItems: "center", gap: 6, background: "transparent",
        border: "none", cursor: "pointer", padding: 0, color: cfg ? C.acc : C.txt,
      }}>
        {open ? <IcChevronDown size={13} /> : <IcChevronRight size={13} />}
        <IcBarChart size={12} />
        <span style={{ fontSize: 11, fontWeight: 500, flex: 1, textAlign: "left" }}>Graphiques par entité</span>
        {cfg && <span style={{ fontSize: 8.5, color: C.acc, border: `0.5px solid ${C.acc}55`,
                               borderRadius: 3, padding: "0 4px" }}>{cfg.vars?.length} var.</span>}
      </button>

      {open && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 8 }}>

          <div style={{ display: "flex", gap: 3, flexWrap: "wrap" }}>
            {CHART_KINDS.map(k => (
              <button key={k.id} onClick={() => setKind(k.id)} style={{ ...btn(kind === k.id), flex: "1 1 auto" }}>
                {k.label}
              </button>
            ))}
          </div>

          {numAttrs.length === 0 ? (
            <div style={{ fontSize: 10, color: C.dim }}>Aucune colonne numérique dans cette couche.</div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 2, maxHeight: 150, overflowY: "auto" }}>
              {numAttrs.map(a => {
                const i = vars.indexOf(a);
                const on = i >= 0;
                return (
                  <div key={a} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10.5, color: C.mut }}>
                    <input type="checkbox" checked={on} style={{ accentColor: C.acc, cursor: "pointer" }}
                      onChange={e => setVars(p => e.target.checked ? [...p, a] : p.filter(x => x !== a))} />
                    {/* Pastille = sélecteur de couleur manuel pour cette variable */}
                    <label title={on ? "Changer la couleur de cette variable" : ""}
                      style={{ width: 13, height: 13, borderRadius: 3, flexShrink: 0, position: "relative",
                               overflow: "hidden", cursor: on ? "pointer" : "default",
                               background: on ? eff[i] : "transparent",
                               border: on ? "0.5px solid rgba(0,0,0,.25)" : `0.5px solid ${C.bdr}` }}>
                      {on && <input type="color" value={eff[i]} onChange={e => setColorAt(i, e.target.value)}
                        style={{ position: "absolute", inset: -4, opacity: 0, cursor: "pointer", border: "none", padding: 0 }} />}
                    </label>
                    <span style={{ flex: 1, minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{a}</span>
                  </div>
                );
              })}
            </div>
          )}

          {/* Palette + inversion */}
          <div>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 4 }}>
              <span style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Palette</span>
              <div style={{ display: "flex", gap: 4 }}>
                <button onClick={() => setInvert(v => !v)} style={{ ...btn(invert), padding: "2px 7px" }}
                  title={invert ? "Palette inversée — cliquer pour rétablir" : "Inverser la palette"}>
                  ↕ {invert ? "Inversée" : "Inverser"}
                </button>
                {colors.some(Boolean) && (
                  <button onClick={() => { setColors([]); if (cfg) push({ colors: [] }); }}
                    style={{ ...btn(false), padding: "2px 7px" }} title="Oublier les couleurs choisies à la main">
                    Réinit.
                  </button>
                )}
              </div>
            </div>
            <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
              {Object.entries(RAMPS).map(([n, cs]) => {
                const prev = invert ? [...cs].reverse() : cs;
                return (
                  <button key={n} onClick={() => setPalette(n)} title={n} style={{
                    width: 42, height: 12, borderRadius: 3, cursor: "pointer", padding: 0,
                    border: palette === n ? `2px solid ${C.acc}` : `1px solid ${C.bdr}`,
                    background: `linear-gradient(to right,${prev.slice(0, 5).join(",")})`,
                  }} />
                );
              })}
            </div>
          </div>

          <div style={{ display: "flex", gap: 3 }}>
            {[["proportional", "Taille ∝ total"], ["fixed", "Taille égale"]].map(([k, l]) => (
              <button key={k} onClick={() => setSize(k)} style={{ ...btn(size === k), flex: 1 }}>{l}</button>
            ))}
          </div>

          {/* Deux opacités INDÉPENDANTES : estomper l'une pour lire l'autre,
              ou basculer franchement de l'une à l'autre d'un clic. */}
          <div style={{ background: C.bg, border: `0.5px solid ${C.bdr}`, borderRadius: 7, padding: 8,
                        display: "flex", flexDirection: "column", gap: 6 }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
              <span style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: ".05em" }}>Visibilité</span>
              <button title="Permuter : montrer l'un, masquer l'autre"
                onClick={() => {
                  const showCharts = opacity < 0.5;   // graphiques estompés → on les met devant
                  const co = showCharts ? 1 : 0, lo = showCharts ? 0 : 1;
                  setOpacity(co); if (cfg) push({ opacity: co });
                  onLayerOpacity?.(lo);
                }}
                style={{ ...btn(false), padding: "2px 8px" }}>⇄ Permuter</button>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ fontSize: 10, color: C.dim, width: 74, flexShrink: 0 }}>Classification</span>
              <input type="range" min="0" max="1" step="0.05" value={layerOpacity}
                onChange={e => onLayerOpacity?.(parseFloat(e.target.value))} style={{ flex: 1, height: 3 }} />
              <span style={{ fontFamily: M, fontSize: 10, color: C.mut, width: 30, textAlign: "right" }}>
                {Math.round(layerOpacity * 100)}%
              </span>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ fontSize: 10, color: C.dim, width: 74, flexShrink: 0 }}>Graphiques</span>
              <input type="range" min="0" max="1" step="0.05" value={opacity}
                onChange={e => { const v = parseFloat(e.target.value); setOpacity(v); if (cfg) push({ opacity: v }); }}
                style={{ flex: 1, height: 3 }} />
              <span style={{ fontFamily: M, fontSize: 10, color: C.mut, width: 30, textAlign: "right" }}>
                {Math.round(opacity * 100)}%
              </span>
            </div>
          </div>

          {/* Facteur global : le réglage à baisser quand tout est affiché et
              que les vignettes se marchent dessus. 0 les masque entièrement. */}
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ fontSize: 10, color: C.dim, flexShrink: 0 }}>Taille ×</span>
            <input type="range" min="0" max="10" step="0.5" value={scale}
              onChange={e => { const v = parseFloat(e.target.value); setScale(v); if (cfg) push({ scale: v }); }}
              style={{ flex: 1, height: 3 }} />
            <span style={{ fontFamily: M, fontSize: 10.5, color: scale === 0 ? C.amb : C.txt, width: 24, textAlign: "right" }}>
              {scale}
            </span>
          </div>
          {scale === 0 && (
            <div style={{ fontSize: 9, color: C.amb, marginTop: -4 }}>Taille nulle : les graphiques sont invisibles.</div>
          )}

          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ fontSize: 10, color: C.dim, flexShrink: 0 }}>Visible dès z</span>
            <input type="range" min="0" max="14" step="1" value={minzoom}
              onChange={e => setMinzoom(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
            <span style={{ fontFamily: M, fontSize: 10.5, color: C.txt, width: 18, textAlign: "right" }}>{minzoom}</span>
          </div>

          {/* Diagnostic : sans lui, un seuil trop haut donne un écran vide sans
              la moindre explication — le cas le plus fréquent. */}
          {zoom !== null && (
            <div style={{ fontSize: 9.5, fontFamily: M, color: hiddenByZoom ? C.amb : C.dim }}>
              zoom actuel {zoom.toFixed(1)} · {eligible} entité(s) avec données
            </div>
          )}
          {hiddenByZoom && (
            <div style={{ display: "flex", alignItems: "center", gap: 7, background: C.amb + "12",
                          border: `0.5px solid ${C.amb}44`, borderRadius: 6, padding: "6px 8px" }}>
              <IcAlert size={12} style={{ color: C.amb, flexShrink: 0 }} />
              <span style={{ flex: 1, fontSize: 9.5, color: C.mut, lineHeight: 1.4 }}>
                Masqués : la carte est au zoom {zoom.toFixed(1)}, le seuil est à {minzoom}.
              </span>
              <button onClick={() => { const z = Math.max(0, Math.floor(zoom)); setMinzoom(z); if (cfg) push({ minzoom: z }); }}
                style={{ ...btn(false), padding: "3px 7px", flexShrink: 0 }}>Afficher ici</button>
            </div>
          )}
          {vars.length > 0 && eligible === 0 && (
            <div style={{ display: "flex", gap: 7, background: C.red + "12", border: `0.5px solid ${C.red}44`,
                          borderRadius: 6, padding: "6px 8px", fontSize: 9.5, color: C.mut, lineHeight: 1.4 }}>
              <IcAlert size={12} style={{ color: C.red, flexShrink: 0 }} />
              Aucune entité n'a de valeur positive sur ces variables : rien ne peut être dessiné.
            </div>
          )}

          <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10, color: C.mut, cursor: "pointer" }}>
            <input type="checkbox" checked={overlap} onChange={e => setOverlap(e.target.checked)}
              style={{ accentColor: C.acc, cursor: "pointer" }} />
            Tout afficher, même superposé
          </label>
          {!overlap && (
            <div style={{ fontSize: 9, color: C.dim, lineHeight: 1.4, marginTop: -4 }}>
              Sinon les graphiques qui se recouvrent sont masqués, les plus gros
              totaux l'emportent, et ils réapparaissent en zoomant.
            </div>
          )}

          <div style={{ display: "flex", gap: 5 }}>
            <button onClick={() => push()} disabled={!enough} style={{
              flex: 2, fontFamily: F, fontSize: 10.5, fontWeight: 600, padding: "7px 0", borderRadius: 6,
              background: enough ? C.acc : C.hover, color: enough ? "#fff" : C.dim,
              border: "none", cursor: enough ? "pointer" : "default",
            }}>{cfg ? "Mettre à jour" : "Appliquer"}</button>
            {cfg && (
              <button onClick={() => onChange(null)} style={{
                flex: 1, fontFamily: F, fontSize: 10, padding: "7px 0", borderRadius: 6,
                background: "transparent", border: `0.5px solid ${C.bdr}`, color: C.mut, cursor: "pointer",
              }}>Retirer</button>
            )}
          </div>
          {!enough && (
            <div style={{ fontSize: 9, color: C.dim, marginTop: -4 }}>
              {kind === "bars" ? "Sélectionnez au moins une variable." : "Ce type demande au moins deux variables."}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
