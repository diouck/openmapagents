/**
 * TimeAnimTab.jsx — Contenu de l'onglet « Animation » de l'IndicatorModal.
 *
 * Anime les TUILES GEE directement sur la carte MapLibre. À ne pas confondre
 * avec TimelapsePanel.jsx, qui pilote la génération d'un GIF côté serveur
 * (backend/gee_timelapse.py, Pillow) : ici rien n'est rendu au serveur, on fait
 * défiler des URLs de tuiles.
 *
 * Panneau en flux (aucune position absolue) : il vit dans la fenêtre flottante,
 * donc déplaçable et redimensionnable avec elle.
 *
 * Trois temps :
 *   1. « config »  → période + pas de temps, puis « Préparer ».
 *   2. « loading » → le plan fixe l'échelle de couleurs commune, puis les frames
 *                    arrivent par lots de 8. La lecture s'active dès le 1er lot,
 *                    le reste continue en tâche de fond (barre de remplissage).
 *   3. « ready »   → transport ⏮ ⏯ ⏭, curseur, vitesse, boucle, export.
 *
 * Les tuiles sont échangées par utils/rasterAnim (ping-pong A/B) : jamais de
 * flash blanc entre deux dates.
 */
import { useState, useRef, useEffect, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, M } from "../config";
import {
  IcPlay, IcPause, IcChevronLeft, IcChevronRight,
  IcRefresh, IcAlert, IcFileDown,
} from "../icons";
import { preloadFrame, commitFrame, clearAnim, setFrameOpacity } from "../utils/rasterAnim";
import {
  STEP_LABELS, stepsFor, defaultStep, defaultRange, vintagePeriods,
  planTimelapse, fetchFrames, exportAnimation, API_BASE,
} from "../utils/timelapse";

const BATCH = 8;                       // frames par lot (chargement progressif)
const SPEEDS = [0.5, 1, 2, 4, 8];      // images/seconde

export default function TimeAnimTab({
  geeParams,            // paramètres courants du modal (dataset, index, emprise, nuages…)
  animId,               // identifiant de base des deux couches MapLibre d'animation (STABLE)
  opacity = 0.85,
  mapRef,
  onActiveChange,       // (bool) → le parent masque/réaffiche la tuile fixe correspondante
  isOptical = false,    // capteur optique → filtre nuages pertinent
  showComposite = true, // certains produits (5P, CHIRPS, ET, SMAP) imposent leur agrégation
  sourceLabel,          // libellé lisible du capteur, pour le crédit de la vidéo
}) {
  const C  = useThemeContext();
  const ds = geeParams?.dataset;

  const steps  = stepsFor(ds);
  const isVint = steps[0] === "vintage";

  const [phase,   setPhase]   = useState("config");
  const [step,    setStep]    = useState(() => (isVint ? "vintage" : defaultStep(ds)));
  const [range,   setRange]   = useState(() => defaultRange(defaultStep(ds)));
  const [maxF,    setMaxF]    = useState(24);
  const [cloud,   setCloud]   = useState(() => geeParams?.cloud_max ?? 20);
  // Médiane par défaut : sur une animation, prendre « l'image la moins nuageuse »
  // de chaque période fait sauter la couverture d'une date à l'autre. La médiane
  // pixel par pixel donne une suite d'images comparables entre elles.
  const [composite, setComposite] = useState("median");
  const [periods, setPeriods] = useState([]);
  const [loaded,  setLoaded]  = useState(0);
  const [idx,     setIdx]     = useState(0);
  const [playing, setPlaying] = useState(false);
  const [buffering, setBuffering] = useState(false);
  const [fps,     setFps]     = useState(2);
  const [loop,    setLoop]    = useState(true);
  const [err,     setErr]     = useState(null);
  const [recording, setRecording] = useState(false);

  const framesRef  = useRef([]);
  const periodsRef = useRef([]);
  const idxRef     = useRef(0);
  const loopRef    = useRef(true);
  const abortRef   = useRef(null);
  const gpRef      = useRef(geeParams);
  const bodyRef    = useRef(null);   // corps de requête du plan, rejoué à l'export

  useEffect(() => { loopRef.current = loop; }, [loop]);
  useEffect(() => { gpRef.current = geeParams; });

  const getMap = useCallback(() => mapRef?.current?.getMap?.(), [mapRef]);

  // Nettoyage : on retire les couches d'animation en quittant l'onglet.
  useEffect(() => () => {
    abortRef.current?.abort();
    const m = getMap(); if (m && animId) clearAnim(m, animId);
    onActiveChange?.(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [getMap, animId]);

  // Précharge en tâche de fond l'image qui suivra `i` : au moment d'avancer, la
  // bascule est immédiate. C'est ce qui fait tenir la cadence demandée.
  const prefetchAfter = useCallback((i) => {
    const n = periodsRef.current.length; if (!n) return;
    const m = getMap(); if (!m) return;
    for (let k = 1; k <= n; k++) {
      const j = (i + k) % n;
      const f = framesRef.current[j];
      if (!f) return;                                  // lot pas encore arrivé
      if (!f.empty && f.tile_url) { preloadFrame(m, animId, f.tile_url); return; }
    }
  }, [getMap, animId]);

  const display = useCallback(async (i) => {
    const f = framesRef.current[i];
    const m = getMap();
    if (!m || !f || f.empty || !f.tile_url) return;
    await preloadFrame(m, animId, f.tile_url);   // instantané si déjà préchargée
    commitFrame(m, animId, opacity);
    prefetchAfter(i);                            // sans await : prépare la suivante
  }, [getMap, animId, opacity, prefetchAfter]);

  useEffect(() => {
    const m = getMap(); if (m && animId) setFrameOpacity(m, animId, opacity);
  }, [getMap, animId, opacity]);

  // ── Préparation ──────────────────────────────────────────────
  const prepare = async () => {
    const gp = gpRef.current || {};
    if (!gp.bbox && !gp.roi_geojson) {
      setErr("Emprise introuvable — cadrez la carte depuis l'onglet Carte."); return;
    }
    setErr(null); setPhase("loading"); setLoaded(0);
    abortRef.current?.abort();
    const ac = new AbortController(); abortRef.current = ac;
    try {
      const explicit = isVint ? vintagePeriods(ds) : null;
      const body = {
        ...gp,
        classify: null, auto_stretch: false,
        // Réglages propres à l'animation : ils priment sur ceux de l'onglet Carte.
        cloud_max: isOptical ? cloud : 100,
        composite,
        step: isVint ? "year" : step,
        date_start: range.date_start, date_end: range.date_end,
        periods: explicit || undefined,
        max_frames: maxF,
      };
      const plan = await planTimelapse(body, ac.signal);
      const P = plan.periods || [];
      if (!P.length) throw new Error("Aucune période à animer.");
      setPeriods(P); periodsRef.current = P;
      framesRef.current = new Array(P.length).fill(null);
      setIdx(0); idxRef.current = 0;

      // Échelle de couleurs figée pour TOUTES les frames (sinon pas d'évolution visible)
      const fb = { ...body, periods: P, vis_params_override: plan.vis_params };
      bodyRef.current = fb;   // réutilisé tel quel par l'export serveur

      for (let off = 0; off < P.length; off += BATCH) {
        const r = await fetchFrames({ ...fb, offset: off, limit: BATCH }, ac.signal);
        (r.frames || []).forEach(f => { framesRef.current[f.index] = f; });
        setLoaded(c => c + (r.frames?.length || 0));
        if (off === 0) {
          setPhase("ready");
          onActiveChange?.(true);
          const first = framesRef.current.findIndex(f => f && !f.empty && f.tile_url);
          if (first >= 0) { setIdx(first); idxRef.current = first; display(first); }
        }
      }
    } catch (e) {
      if (e.name === "AbortError") return;
      setErr(e.message); setPhase("config");
    }
  };

  // ── Navigation ───────────────────────────────────────────────
  // Renvoie { i } prête, { wait } pas encore chargée, ou { end } fin de piste.
  const findNext = useCallback((cur, dir) => {
    const n = periodsRef.current.length;
    if (!n) return { end: true };
    for (let k = 1; k <= n; k++) {
      let j = cur + dir * k;
      if ((j >= n || j < 0) && !loopRef.current) return { end: true };
      j = ((j % n) + n) % n;
      const f = framesRef.current[j];
      if (!f) return { wait: true };                 // lot pas encore arrivé
      if (!f.empty && f.tile_url) return { i: j };   // sinon période sans image → on saute
    }
    return { end: true };
  }, []);

  const goto = useCallback(async (i) => {
    setIdx(i); idxRef.current = i; await display(i);
  }, [display]);

  const stepBy = (dir) => {
    const r = findNext(idxRef.current, dir);
    if (r.i != null) goto(r.i);
  };

  // Boucle de lecture : pilotée par des refs pour que l'arrivée d'un lot ne la
  // redémarre pas (ce qui produirait un à-coup).
  useEffect(() => {
    if (!playing) return;
    let cancelled = false, t;
    const interval = Math.round(1000 / fps);
    const tick = async () => {
      if (cancelled) return;
      const r = findNext(idxRef.current, 1);
      if (r.end)  { setPlaying(false); return; }
      if (r.wait) { setBuffering(true); t = setTimeout(tick, 350); return; }
      setBuffering(false);
      setIdx(r.i); idxRef.current = r.i;
      // Attente PARALLÈLE : on tient max(chargement, cadence) et non leur somme.
      // C'est la somme qui faisait durer 31 s une animation de 24 images à 2/s.
      await Promise.all([display(r.i), new Promise(res => setTimeout(res, interval))]);
      if (cancelled) return;
      t = setTimeout(tick, 0);
    };
    t = setTimeout(tick, interval);
    return () => { cancelled = true; clearTimeout(t); };
  }, [playing, fps, findNext, display]);

  // Titre : « NDVI de 2000 à 2025 » — construit sur les libellés réels des bornes.
  const exportTitle = () => {
    const P = periodsRef.current;
    const idxLbl = geeParams?.index || "Indice";
    if (!P.length) return idxLbl;
    return P.length > 1 ? `${idxLbl} de ${P[0].label} à ${P[P.length - 1].label}` : `${idxLbl} · ${P[0].label}`;
  };

  // ── Export : assemblé CÔTÉ SERVEUR ──────────────────────────
  // Capturer le canvas via MediaRecorder donnait un conteneur fragmenté sans
  // durée, que la plupart des lecteurs réduisaient à une seule image. On délègue
  // donc au serveur, comme le module GIF : une vignette GEE par date, annotée
  // (titre, date, source, nord, échelle) puis assemblée en fichier fini.
  // Contrepartie assumée : le fichier ne contient que la couche GEE, sans fond
  // de plan — l'aperçu sur la carte, lui, garde le fond.
  const exportFile = async () => {
    const fb = bodyRef.current;
    if (!fb) { setErr("Préparez d'abord l'animation."); return; }
    setPlaying(false); setRecording(true); setErr(null);
    try {
      const d = await exportAnimation({
        ...fb,
        fps: Math.max(1, Math.min(Math.round(fps), 15)),
        width: 720,
        title: exportTitle(),
        credits: `${sourceLabel || ds || ""} · OpenMapAgents · GEE`.replace(/^ · /, ""),
        fmt: "gif",
      });
      const a = document.createElement("a");
      a.href = `${API_BASE}${d.url}`;
      a.download = "";
      a.target = "_blank";
      a.click();
      setErr(d.skipped
        ? `✓ ${d.frames} images (${d.size_mb} Mo) — ${d.skipped} date(s) sans image ignorée(s).`
        : `✓ ${d.frames} images · ${d.size_mb} Mo`);
    } catch (e) {
      setErr(`Export : ${e.message}`);
    }
    setRecording(false);
  };

  // ── Styles ───────────────────────────────────────────────────
  const btn = (on = false) => ({
    fontFamily: F, fontSize: 10.5, padding: "5px 8px", borderRadius: 6,
    background: on ? C.acc + "20" : "transparent",
    border: `0.5px solid ${on ? C.acc + "77" : C.bdr}`,
    color: on ? C.acc : C.mut, cursor: "pointer",
    display: "flex", alignItems: "center", justifyContent: "center", gap: 4, flexShrink: 0,
  });
  const inp = {
    fontFamily: M, fontSize: 11, padding: "5px 7px", borderRadius: 6, width: "100%",
    background: C.input, color: C.txt, border: `0.5px solid ${C.bdr}`, outline: "none",
    boxSizing: "border-box",
  };
  const lbl = { fontSize: 9, color: C.dim, marginBottom: 4, textTransform: "uppercase", letterSpacing: ".05em" };

  const n   = periods.length;
  const cur = periods[idx];
  const pct = n ? Math.round(loaded / n * 100) : 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>

      {err && (
        <div style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10.5,
                      color: C.red, background: C.red + "12", borderRadius: 6, padding: "6px 8px" }}>
          <IcAlert size={12} /> {err}
        </div>
      )}

      {/* ── Réglages ── */}
      {phase === "config" && (
        <>
          <div>
            <div style={lbl}>Pas de temps</div>
            <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
              {(isVint ? ["vintage"] : steps).map(s => (
                <button key={s} onClick={() => { setStep(s); if (s !== "vintage") setRange(defaultRange(s)); }}
                  style={{ ...btn(step === s), flex: "1 1 auto" }}>
                  {s === "vintage" ? "Millésimes" : STEP_LABELS[s]}
                </button>
              ))}
            </div>
          </div>

          {!isVint && (
            <>
              <div style={{ display: "flex", gap: 8 }}>
                <div style={{ flex: 1 }}>
                  <div style={lbl}>Du</div>
                  <input type="date" value={range.date_start} style={inp}
                    onChange={e => setRange(r => ({ ...r, date_start: e.target.value }))} />
                </div>
                <div style={{ flex: 1 }}>
                  <div style={lbl}>Au</div>
                  <input type="date" value={range.date_end} style={inp}
                    onChange={e => setRange(r => ({ ...r, date_end: e.target.value }))} />
                </div>
              </div>
              <div>
                <div style={lbl}>Maximum {maxF} images</div>
                <input type="range" min="4" max="60" step="2" value={maxF}
                  onChange={e => setMaxF(Number(e.target.value))} style={{ width: "100%" }} />
              </div>
            </>
          )}

          {/* Nuages — propre à l'animation : un seuil serré vide des périodes */}
          {isOptical && (
            <div>
              <div style={lbl}>Nuages max</div>
              <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                <input type="range" min="0" max="100" step="5" value={cloud}
                  onChange={e => setCloud(parseInt(e.target.value))} style={{ flex: 1, height: 3 }} />
                <span style={{ fontFamily: M, fontSize: 11, color: C.txt, width: 34, textAlign: "right" }}>{cloud} %</span>
              </div>
              <div style={{ fontSize: 9.5, color: C.dim, marginTop: 4, lineHeight: 1.5 }}>
                Trop bas, des dates se retrouvent sans image : elles sont sautées et
                l'animation devient irrégulière.
              </div>
            </div>
          )}

          {/* Composite calculé sur CHAQUE période, pas sur l'ensemble */}
          {showComposite && (
            <div>
              <div style={lbl}>Composite par date</div>
              <div style={{ display: "flex", gap: 4 }}>
                {[["least_cloudy", "Moins nuageux"], ["median", "Médiane"], ["mosaic", "Mosaïque"]].map(([k, l]) => (
                  <button key={k} onClick={() => setComposite(k)} title={
                    k === "least_cloudy" ? "Image la moins nuageuse de chaque période"
                    : k === "median" ? "Médiane pixel par pixel — images comparables d'une date à l'autre"
                    : "Assemblage des images les plus récentes de la période"
                  } style={{ ...btn(composite === k), flex: 1, padding: "6px 4px" }}>{l}</button>
                ))}
              </div>
            </div>
          )}

          <div style={{ fontSize: 10, color: C.dim, lineHeight: 1.55 }}>
            L'emprise et le satellite sont ceux de l'onglet Carte.
            L'échelle de couleurs est calculée une seule fois sur toute la période,
            sinon chaque image se renormalise et l'évolution devient invisible.
          </div>

          <button onClick={prepare} style={{
            width: "100%", fontFamily: F, fontSize: 11.5, fontWeight: 600, padding: "9px 0",
            borderRadius: 7, background: C.acc, color: "#fff", border: "none", cursor: "pointer",
          }}>
            Préparer l'animation
          </button>
        </>
      )}

      {phase === "loading" && (
        <div style={{ fontSize: 11, color: C.mut, padding: "6px 0", lineHeight: 1.6 }}>
          Calcul de l'échelle commune et des premières dates…
        </div>
      )}

      {/* ── Lecteur ── */}
      {phase === "ready" && (
        <>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <span style={{ fontFamily: M, fontSize: 14, fontWeight: 600, color: C.txt }}>
              {buffering
                ? <span style={{ color: C.dim, fontSize: 11 }}>mise en tampon…</span>
                : (cur?.label || "—")}
            </span>
            <span style={{ fontFamily: M, fontSize: 9.5, color: C.dim }}>
              {loaded < n ? `${loaded}/${n} chargées` : `${n} dates`}
            </span>
          </div>

          {/* Curseur + remplissage du chargement */}
          <div style={{ position: "relative" }}>
            <div style={{ position: "absolute", left: 0, right: 0, top: "50%", height: 3,
                          transform: "translateY(-50%)", background: C.bdr, borderRadius: 2 }}>
              <div style={{ width: `${pct}%`, height: "100%", background: C.acc + "55", borderRadius: 2 }} />
            </div>
            <input type="range" min="0" max={Math.max(0, n - 1)} value={idx}
              onChange={e => { setPlaying(false); goto(Number(e.target.value)); }}
              style={{ width: "100%", position: "relative", background: "transparent" }} />
          </div>

          <div style={{ display: "flex", gap: 6 }}>
            <button onClick={() => stepBy(-1)} style={{ ...btn(), flex: 1 }} title="Date précédente"><IcChevronLeft size={15} /></button>
            <button onClick={() => setPlaying(p => !p)} style={{ ...btn(playing), flex: 2 }}>
              {playing ? <><IcPause size={14} /> Pause</> : <><IcPlay size={14} /> Lecture</>}
            </button>
            <button onClick={() => stepBy(1)} style={{ ...btn(), flex: 1 }} title="Date suivante"><IcChevronRight size={15} /></button>
          </div>

          <div>
            <div style={lbl}>Vitesse</div>
            <div style={{ display: "flex", gap: 4 }}>
              {SPEEDS.map(s => (
                <button key={s} onClick={() => setFps(s)} style={{ ...btn(fps === s), flex: 1 }}>{s}×</button>
              ))}
            </div>
          </div>

          <div style={{ display: "flex", gap: 6 }}>
            <button onClick={() => setLoop(v => !v)} style={{ ...btn(loop), flex: 1 }} title="Lecture en boucle">
              <IcRefresh size={12} /> Boucle
            </button>
            <button onClick={() => { setPhase("config"); setPlaying(false); }} style={{ ...btn(), flex: 1 }}>
              Régler
            </button>
            <button onClick={exportFile} disabled={recording}
              title="Génère le fichier animé sur le serveur (couche GEE annotée, sans fond de plan)"
              style={{ ...btn(), flex: 1, opacity: recording ? 0.45 : 1 }}>
              <IcFileDown size={12} /> {recording ? "Génération…" : "Exporter"}
            </button>
          </div>
        </>
      )}
    </div>
  );
}
