/**
 * SaveMapModal.jsx — version finale
 * - mapStyle sauvegardé dans state_json (fond de carte reproductible)
 * - miniature : initialThumb prop ou capture canvas triple rAF
 * - isValidUUID() → jamais PATCH /undefined
 * - apiFetch local (pas de dépendance useAuth().authFetch)
 * - déconnexion dans header
 * - toggle Mettre à jour / Nouvelle carte
 */
import { useState, useEffect } from "react";
import { useThemeContext } from "../theme";
import { F, API } from "../config";
import { IcEdit, IcSave, IcX, IcPlus, IcMap, IcBoxes, IcPalette, IcHexagon, IcCheck, IcImage, IcShare, IcHash } from "../icons";
import { useAuth } from "../useAuth";

function isValidUUID(id) {
  if (!id || id === "undefined" || id === "null") return false;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(id));
}

async function apiFetch(url, opts = {}) {
  let token = localStorage.getItem("oma_access");
  let r = await fetch(url, {
    ...opts,
    headers: { ...(opts.headers || {}), Authorization: `Bearer ${token}` },
  });
  if (r.status === 401) {
    const refresh = localStorage.getItem("oma_refresh");
    if (refresh) {
      const rr = await fetch(`${API}/auth/refresh`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refresh_token: refresh }),
      });
      if (rr.ok) {
        const tokens = await rr.json();
        localStorage.setItem("oma_access",  tokens.access_token);
        localStorage.setItem("oma_refresh", tokens.refresh_token);
        r = await fetch(url, {
          ...opts,
          headers: { ...(opts.headers || {}), Authorization: `Bearer ${tokens.access_token}` },
        });
      }
    }
  }
  return r;
}

export default function SaveMapModal({
  mapRef,
  layers,
  viewport,
  mapStyle,      // ← clé du fond de carte ("dark"|"liberty"|"positron")
  existingMap,
  initialThumb,
  onClose,
  onSaved,
}) {
  const C = useThemeContext();
  const { user, logout } = useAuth();

  const hasValidMap = isValidUUID(existingMap?.id);

  const [mode,   setMode]   = useState(hasValidMap ? "update" : "new");
  const [title,  setTitle]  = useState(existingMap?.title || "");
  const [desc,   setDesc]   = useState(existingMap?.description || "");
  const [pub,    setPub]    = useState(existingMap?.is_public || false);
  const [thumb,  setThumb]  = useState("");  // sera rempli par useEffect
  const [busy,   setBusy]   = useState(false);
  const [err,    setErr]    = useState("");
  const [copied, setCopied] = useState(false);

  // ── Capture miniature robuste ─────────────────────────────
  useEffect(() => {
    // Si une miniature valide est passée en prop → l'utiliser directement
    if (initialThumb && initialThumb.length > 5000) {
      setThumb(initialThumb);
      return;
    }
    // Sinon capturer depuis le canvas MapLibre
    const map = mapRef?.current?.getMap?.();
    if (!map) return;

    const doCapture = () => {
      map.triggerRepaint();
      requestAnimationFrame(() => requestAnimationFrame(() => requestAnimationFrame(() => {
        try {
          const data = map.getCanvas().toDataURL("image/jpeg", 0.85);
          if (data && data.length > 5000) setThumb(data);
        } catch {}
      })));
    };

    if (!map.loaded() || map.isMoving() || map.isZooming() || map.isRotating()) {
      map.once("idle", doCapture);
    } else {
      setTimeout(doCapture, 150);
    }
  }, [initialThumb]);

  // ── buildState — inclut mapStyle pour reproduction exacte ──
  const buildState = () => ({
    layers: (layers || []).map(l => ({
      id:       l.id,
      name:     l.name,
      theme:    l.theme,
      visible:  l.visible,
      color:    l.color,
      opacity:  l.opacity,
      radius:   l.radius,
      classCfg: l.classCfg,
      geojson:  l.geojson,
    })),
    viewport: {
      longitude: viewport?.longitude,
      latitude:  viewport?.latitude,
      zoom:      viewport?.zoom    ?? 12,
      pitch:     viewport?.pitch   ?? 0,
      bearing:   viewport?.bearing ?? 0,
    },
    mapStyle: mapStyle || "positron",   // ← fond de carte sauvegardé
    savedAt: new Date().toISOString(),
  });

  // ── Save ───────────────────────────────────────────────────
  const save = async () => {
    if (!title.trim()) { setErr("Titre requis"); return; }
    setBusy(true); setErr("");
    try {
      const body = {
        title: title.trim(), description: desc.trim(),
        state_json: buildState(), thumbnail: thumb, is_public: pub,
      };
      const isUpdate = mode === "update" && hasValidMap;
      const r = await apiFetch(
        isUpdate ? `${API}/maps/my/${existingMap.id}` : `${API}/maps`,
        { method: isUpdate ? "PATCH" : "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body) }
      );
      const data = await r.json();
      if (!r.ok) throw new Error(data.detail || "Erreur sauvegarde");
      onSaved?.(data);
      onClose();
    } catch (e) {
      setErr(e.message);
    } finally { setBusy(false); }
  };

  const doLogout = () => { logout(); onClose(); };

  const shareUrl = pub && hasValidMap && existingMap?.slug
    ? `${window.location.origin}/share/${existingMap.slug}` : null;

  const copyLink = () => {
    navigator.clipboard.writeText(shareUrl);
    setCopied(true); setTimeout(() => setCopied(false), 2000);
  };

  const inp = {
    fontFamily: F, fontSize: 12, padding: "9px 12px", borderRadius: 7,
    width: "100%", boxSizing: "border-box",
    background: C.input, color: C.txt, border: `1px solid ${C.bdr}`, outline: "none",
  };

  const layerCount   = layers?.length || 0;
  const hasClass     = layers?.some(l => l.classCfg);
  const featureCount = layers?.reduce((a, l) => a + (l.featureCount || l.geojson?.features?.length || 0), 0) || 0;

  return (
    <>
      <div onClick={onClose} style={{
        position: "fixed", inset: 0, background: "rgba(0,0,0,0.55)",
        zIndex: 11000, backdropFilter: "blur(4px)",
      }} />

      <div style={{
        position: "fixed", top: "50%", left: "50%",
        transform: "translate(-50%,-50%)",
        zIndex: 11001, background: C.card,
        border: `1px solid ${C.bdr}`, borderRadius: 14,
        boxShadow: "0 24px 64px rgba(0,0,0,0.4)",
        width: 460, maxWidth: "94vw", maxHeight: "90vh", overflowY: "auto", fontFamily: F,
      }}>

        {/* Header */}
        <div style={{
          display: "flex", alignItems: "flex-start",
          justifyContent: "space-between", padding: "18px 20px 0",
        }}>
          <div>
            <div style={{ fontSize: 15, fontWeight: 700, color: C.txt, display: "flex", alignItems: "center", gap: 7 }}>
              {mode === "update" ? <><IcEdit size={16}/> Mettre à jour la carte</> : <><IcSave size={16}/> Nouvelle carte</>}
            </div>
            {hasValidMap && (
              <div style={{ fontSize: 10, color: C.dim, marginTop: 2 }}>
                {mode === "update" ? `Écrase "${existingMap.title}"` : "Copie indépendante"}
              </div>
            )}
          </div>

          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            {user && (
              <div style={{ textAlign: "right" }}>
                <div style={{ fontSize: 11, fontWeight: 600, color: C.txt }}>{user.username}</div>
                <button onClick={doLogout} style={{
                  fontFamily: F, fontSize: 10, padding: "2px 8px",
                  borderRadius: 4, cursor: "pointer",
                  background: "transparent", color: C.dim, border: `1px solid ${C.bdr}`,
                }}>Se déconnecter</button>
              </div>
            )}
            <button onClick={onClose} style={{
              background: "none", border: "none",
              color: C.dim, cursor: "pointer", display: "flex", padding: 2,
            }}><IcX size={18}/></button>
          </div>
        </div>

        {/* Toggle update / nouvelle */}
        {hasValidMap && (
          <div style={{
            display: "flex", gap: 4, margin: "14px 20px 0",
            background: C.bg, borderRadius: 8, padding: 3,
          }}>
            {[["update",IcEdit,"Mettre à jour"],["new",IcPlus,"Nouvelle carte"]].map(([k, Icon, label]) => (
              <button key={k}
                onClick={() => { setMode(k); setErr(""); setTitle(k === "new" ? "" : (existingMap?.title || "")); }}
                style={{
                  flex: 1, fontFamily: F, fontSize: 11, fontWeight: 500,
                  padding: "6px 0", borderRadius: 6, cursor: "pointer",
                  background: mode === k ? C.card : "transparent",
                  color: mode === k ? C.txt : C.dim,
                  border: mode === k ? `1px solid ${C.bdr}` : "1px solid transparent",
                  boxShadow: mode === k ? "0 1px 4px rgba(0,0,0,0.1)" : "none",
                  transition: "all 0.15s",
                  display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 5,
                }}
              ><Icon size={13}/> {label}</button>
            ))}
          </div>
        )}

        <div style={{ padding: "14px 20px 20px", display: "flex", flexDirection: "column", gap: 12 }}>

          {/* Miniature */}
          <div style={{
            borderRadius: 8, overflow: "hidden",
            border: `1px solid ${C.bdr}`, height: 130,
            background: C.bg, position: "relative",
          }}>
            {thumb ? (
              <img src={thumb} alt="Aperçu"
                style={{ width: "100%", height: "100%", objectFit: "cover" }} />
            ) : (
              <div style={{
                width: "100%", height: "100%",
                display: "flex", flexDirection: "column",
                alignItems: "center", justifyContent: "center",
                gap: 6, fontSize: 11, color: C.dim,
              }}>
                <IcMap size={24}/>
                Génération de l'aperçu…
              </div>
            )}
            {/* Badge fond de carte */}
            {mapStyle && (
              <div style={{
                position: "absolute", bottom: 6, right: 6,
                background: "rgba(0,0,0,0.6)", borderRadius: 4,
                padding: "2px 6px", fontSize: 9, color: "#fff",
              }}>{mapStyle}</div>
            )}
          </div>

          {/* Titre */}
          <div>
            <div style={{ fontSize: 10, color: C.dim, marginBottom: 4, fontWeight: 500 }}>TITRE *</div>
            <input style={inp} placeholder="Ex: Population Afrique 2024"
              value={title} onChange={e => setTitle(e.target.value)} maxLength={200} />
          </div>

          {/* Description */}
          <div>
            <div style={{ fontSize: 10, color: C.dim, marginBottom: 4, fontWeight: 500 }}>DESCRIPTION</div>
            <textarea style={{ ...inp, height: 60, resize: "vertical" }}
              placeholder="Décrivez votre carte (optionnel)…"
              value={desc} onChange={e => setDesc(e.target.value)} maxLength={500} />
          </div>

          {/* Contenu sauvegardé */}
          <div style={{
            padding: "10px 12px", borderRadius: 8,
            background: C.bg, border: `1px solid ${C.bdr}`,
            fontSize: 11, color: C.dim, lineHeight: 1.8,
          }}>
            <div style={{ fontWeight: 600, color: C.txt, marginBottom: 4, fontSize: 11, display: "flex", alignItems: "center", gap: 6 }}>
              <IcBoxes size={13}/> Contenu sauvegardé
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}><IcMap size={12}/> <span>Fond de carte <strong style={{ color: C.txt }}>{mapStyle || "positron"}</strong> · position · zoom{viewport?.pitch > 0 ? " · pitch 3D" : ""}</span></div>
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}><IcPalette size={12}/> Thème UI</div>
            {layerCount > 0 ? (
              <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <IcHexagon size={12}/> <span>{layerCount} couche{layerCount > 1 ? "s" : ""} vectorielle{layerCount > 1 ? "s" : ""}
                {featureCount > 0 && ` (${featureCount.toLocaleString("fr")} entités)`}
                {" — "}<span style={{ color: "#22c55e", fontWeight: 500 }}>géométries incluses</span></span>
                <IcCheck size={12} color="#22c55e"/>
              </div>
            ) : (
              <div style={{ color: C.dim, display: "flex", alignItems: "center", gap: 6 }}><IcMap size={12}/> Carte de fond uniquement — sans données importées</div>
            )}
            {hasClass && <div style={{ display: "flex", alignItems: "center", gap: 6 }}><IcHash size={12}/> Discrétisations / classes</div>}
            {thumb && <div style={{ display: "flex", alignItems: "center", gap: 6 }}><IcImage size={12}/> Miniature</div>}
          </div>

          {/* Toggle public */}
          <div style={{
            display: "flex", alignItems: "center", justifyContent: "space-between",
            padding: "10px 12px", background: C.bg,
            borderRadius: 8, border: `1px solid ${C.bdr}`,
          }}>
            <div>
              <div style={{ fontSize: 12, color: C.txt, fontWeight: 500, display: "flex", alignItems: "center", gap: 6 }}><IcShare size={13}/> Vignette partageable</div>
              <div style={{ fontSize: 10, color: C.dim }}>Visible dans la galerie publique</div>
            </div>
            <div onClick={() => setPub(v => !v)} style={{
              width: 38, height: 22, borderRadius: 11, cursor: "pointer",
              background: pub ? C.acc : C.bdr, position: "relative", transition: "background .2s",
            }}>
              <div style={{
                position: "absolute", top: 3, left: pub ? 19 : 3,
                width: 16, height: 16, borderRadius: "50%",
                background: "#fff", transition: "left .2s",
              }} />
            </div>
          </div>

          {/* Lien partage */}
          {shareUrl && (
            <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
              <div style={{
                ...inp, flex: 1, color: C.dim, fontSize: 10, padding: "7px 10px",
                overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
              }}>{shareUrl}</div>
              <button onClick={copyLink} style={{
                fontFamily: F, fontSize: 11, padding: "7px 12px",
                borderRadius: 7, cursor: "pointer", flexShrink: 0,
                background: copied ? "#22c55e" : C.acc, color: "#fff", border: "none",
                transition: "background .2s",
                display: "inline-flex", alignItems: "center", gap: 4,
              }}>{copied ? <><IcCheck size={12}/> Copié</> : "Copier"}</button>
            </div>
          )}

          {/* Erreur */}
          {err && (
            <div style={{
              fontSize: 11, color: "#e05", padding: "7px 10px",
              background: "#e0550011", borderRadius: 6, border: "1px solid #e0550033",
            }}>{err}</div>
          )}

          {/* Bouton save */}
          <button onClick={save} disabled={busy} style={{
            fontFamily: F, fontSize: 13, fontWeight: 600, padding: "11px",
            borderRadius: 9, cursor: busy ? "wait" : "pointer",
            background: busy ? C.dim : C.acc, color: "#fff", border: "none",
            marginTop: 2, opacity: busy ? 0.7 : 1,
            display: "flex", alignItems: "center", justifyContent: "center", gap: 7,
          }}>
            {busy ? "Sauvegarde…"
              : mode === "update" ? <><IcEdit size={14}/> Mettre à jour</> : <><IcSave size={14}/> Sauvegarder la carte</>}
          </button>

        </div>
      </div>
    </>
  );
}
