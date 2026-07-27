/**
 * Dashboard.jsx — Mes cartes + Galerie publique + (Admin) Toutes les cartes
 * Props :
 *   onOpenMap(map)     → charger la carte dans l'app
 *   onUpdateMap(map)   → ouvrir SaveMapModal sur cette carte
 *   onClose()
 */
import { useState, useEffect, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F, API } from "../config";
import { useAuth } from "../useAuth";
import { IcMap, IcShare, IcUser, IcEdit, IcEye, IcLock, IcTrash, IcStack, IcGlobe, IcSettings, IcX, IcSave } from "../icons";

async function apiFetch(url, opts = {}) {
  let token = localStorage.getItem("oma_access");
  const r = await fetch(url, {
    ...opts,
    headers: { ...(opts.headers || {}), Authorization: `Bearer ${token}` },
  });
  return r;
}

// ── Carte vignette ────────────────────────────────────────────
function MapCard({ map, onOpen, onUpdate, onDelete, onTogglePublic, isAdmin = false, C }) {
  const [deleting, setDeleting] = useState(false);

  const handleDelete = async (e) => {
    e.stopPropagation();
    if (!confirm(`Supprimer "${map.title}" ?`)) return;
    setDeleting(true);
    await onDelete(map.id);
  };

  const handleTogglePublic = async (e) => {
    e.stopPropagation();
    await onTogglePublic(map.id, !map.is_public);
  };

  const date = new Date(map.updated_at).toLocaleDateString("fr", {
    day: "numeric", month: "short", year: "numeric",
  });

  return (
    <div
      onClick={() => onOpen(map)}
      style={{
        borderRadius: 10, overflow: "hidden",
        border: `1px solid ${C.bdr}`,
        background: C.card,
        cursor: "pointer",
        transition: "transform .15s, box-shadow .15s",
        opacity: deleting ? 0.4 : 1,
      }}
      onMouseEnter={e => { e.currentTarget.style.transform = "translateY(-2px)"; e.currentTarget.style.boxShadow = "0 8px 24px rgba(0,0,0,0.3)"; }}
      onMouseLeave={e => { e.currentTarget.style.transform = ""; e.currentTarget.style.boxShadow = ""; }}
    >
      {/* Miniature */}
      <div style={{ position: "relative", height: 140, background: C.bg, overflow: "hidden" }}>
        {map.thumbnail ? (
          <img
            src={map.thumbnail}
            alt={map.title}
            style={{ width: "100%", height: "100%", objectFit: "cover" }}
          />
        ) : (
          <div style={{
            width: "100%", height: "100%",
            display: "flex", alignItems: "center", justifyContent: "center",
            opacity: 0.2,
          }}><IcMap size={32}/></div>
        )}

        {/* Badge public */}
        {map.is_public && (
          <div style={{
            position: "absolute", top: 8, right: 8,
            background: "rgba(0,0,0,0.65)", borderRadius: 5,
            padding: "2px 7px", fontSize: 9, color: "#22c55e", fontWeight: 600,
            display: "flex", alignItems: "center", gap: 4,
          }}><IcShare size={10}/> PUBLIC</div>
        )}

        {/* Badge admin : propriétaire */}
        {isAdmin && map.username && (
          <div style={{
            position: "absolute", top: 8, left: 8,
            background: "rgba(0,0,0,0.65)", borderRadius: 5,
            padding: "2px 7px", fontSize: 9, color: "#f59e0b",
            display: "flex", alignItems: "center", gap: 4,
          }}><IcUser size={10}/> {map.username}</div>
        )}

        {/* Bouton modifier — hover sur la miniature */}
        {onUpdate && (
          <button
            onClick={e => { e.stopPropagation(); onUpdate(map); }}
            title="Modifier cette carte"
            style={{
              position: "absolute", bottom: 8, right: 8,
              background: "rgba(0,0,0,0.65)", border: "none",
              borderRadius: 6, padding: "4px 8px",
              fontSize: 12, color: "#fff", cursor: "pointer",
              opacity: 0,
              transition: "opacity .15s",
            }}
            onMouseEnter={e => e.currentTarget.style.opacity = "1"}
            onMouseLeave={e => e.currentTarget.style.opacity = "0"}
            ref={el => {
              // Afficher le bouton au hover du parent
              if (el) {
                const parent = el.closest("[data-card]");
              }
            }}
          ><IcEdit size={14}/></button>
        )}
      </div>

      {/* Infos */}
      <div style={{ padding: "10px 12px 12px" }}>
        <div style={{
          fontSize: 13, fontWeight: 700, color: C.txt,
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          marginBottom: 2,
        }}>{map.title}</div>
        {map.description && (
          <div style={{
            fontSize: 10, color: C.dim, marginBottom: 4,
            overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          }}>{map.description}</div>
        )}
        <div style={{ fontSize: 10, color: C.dim, marginBottom: 8 }}>
          {date}
          {map.view_count > 0 && <span style={{ marginLeft: 8, display: "inline-flex", alignItems: "center", gap: 3 }}><IcEye size={11}/> {map.view_count}</span>}
        </div>

        {/* Actions */}
        <div style={{ display: "flex", gap: 6 }} onClick={e => e.stopPropagation()}>
          <button
            onClick={handleTogglePublic}
            title={map.is_public ? "Rendre privée" : "Partager publiquement"}
            style={{
              flex: 1, fontFamily: F, fontSize: 10, padding: "5px 0",
              borderRadius: 6, cursor: "pointer",
              background: map.is_public ? "#22c55e18" : C.bg,
              color: map.is_public ? "#22c55e" : C.dim,
              border: `1px solid ${map.is_public ? "#22c55e44" : C.bdr}`,
            }}
          >
            <span style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
              {map.is_public ? <><IcShare size={11}/> Partager</> : <><IcLock size={11}/> Privée</>}
            </span>
          </button>

          {onUpdate && (
            <button
              onClick={() => onUpdate(map)}
              title="Modifier"
              style={{
                fontFamily: F, fontSize: 10, padding: "5px 10px",
                borderRadius: 6, cursor: "pointer",
                background: C.bg, color: C.dim,
                border: `1px solid ${C.bdr}`, display: "flex", alignItems: "center",
              }}
            ><IcEdit size={13}/></button>
          )}

          <button
            onClick={handleDelete}
            title="Supprimer"
            disabled={deleting}
            style={{
              fontFamily: F, fontSize: 10, padding: "5px 8px",
              borderRadius: 6, cursor: "pointer",
              background: "#ef444411", color: "#ef4444", border: "none",
              display: "flex", alignItems: "center",
            }}
          ><IcTrash size={13}/></button>
        </div>
      </div>
    </div>
  );
}

// ── Dashboard principal ───────────────────────────────────────
export default function Dashboard({ onOpenMap, onUpdateMap, onClose }) {
  const C = useThemeContext();
  const { user } = useAuth();
  const isAdmin = user?.is_admin;

  // tabs : "mine" | "gallery" | "admin" (si is_admin)
  const [tab,     setTab]     = useState("mine");
  const [maps,    setMaps]    = useState([]);
  const [total,   setTotal]   = useState(0);
  const [page,    setPage]    = useState(1);
  const [loading, setLoading] = useState(false);
  const [search,  setSearch]  = useState("");
  const [publicCount, setPublicCount] = useState(0);
  const PUBLIC_LIMIT = 10;

  const load = useCallback(async (p = 1, reset = true) => {
    setLoading(true);
    try {
      let url;
      if (tab === "mine") {
        url = `${API}/maps?page=${p}&limit=12`;
      } else if (tab === "gallery") {
        url = `${API}/maps/gallery?page=${p}&limit=12`;
      } else if (tab === "admin") {
        url = `${API}/admin/maps?page=${p}&limit=20&search=${encodeURIComponent(search)}`;
      }
      const r = await apiFetch(url);
      const data = await r.json();
      const newMaps = data.maps || [];
      setMaps(prev => reset ? newMaps : [...prev, ...newMaps]);
      setTotal(data.total || 0);
      setPage(p);
      if (data.public_count !== undefined) setPublicCount(data.public_count);
    } catch (e) {
      console.error("Dashboard load:", e);
    } finally {
      setLoading(false);
    }
  }, [tab, search]);

  useEffect(() => { load(1, true); }, [tab]);

  const deleteMap = async (id) => {
    await apiFetch(`${API}/maps/my/${id}`, { method: "DELETE" });
    setMaps(prev => prev.filter(m => m.id !== id));
    setTotal(t => t - 1);
  };

  const togglePublic = async (id, isPublic) => {
    if (isPublic && publicCount >= PUBLIC_LIMIT) {
      alert(`Limite atteinte : ${PUBLIC_LIMIT} vignettes partageables maximum.`);
      return;
    }
    const r = await apiFetch(`${API}/maps/my/${id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ is_public: isPublic }),
    });
    if (r.ok) {
      const data = await r.json();
      setMaps(prev => prev.map(m => m.id === id ? { ...m, is_public: data.is_public } : m));
      setPublicCount(prev => isPublic ? prev + 1 : Math.max(0, prev - 1));
    }
  };

  const filtered = search && tab === "mine"
    ? maps.filter(m => m.title?.toLowerCase().includes(search.toLowerCase()))
    : maps;

  const tabs = [
    { id: "mine",    label: "Mes cartes", icon: IcMap },
    { id: "gallery", label: "Galerie", icon: IcGlobe },
    ...(isAdmin ? [{ id: "admin", label: "Toutes", icon: IcSettings }] : []),
  ];

  const inp = {
    fontFamily: F, fontSize: 12, padding: "7px 10px", borderRadius: 7,
    background: C.input, color: C.txt, border: `1px solid ${C.bdr}`,
    outline: "none", width: "100%", boxSizing: "border-box",
  };

  return (
    <>
      {/* Backdrop */}
      <div onClick={onClose} style={{
        position: "fixed", inset: 0,
        background: "rgba(0,0,0,0.55)", backdropFilter: "blur(4px)",
        zIndex: 10000,
      }} />

      {/* Panel */}
      <div style={{
        position: "fixed", top: "5vh", left: "50%",
        transform: "translateX(-50%)",
        zIndex: 10001, background: C.card,
        border: `1px solid ${C.bdr}`, borderRadius: 14,
        boxShadow: "0 24px 64px rgba(0,0,0,0.5)",
        width: "min(900px, 95vw)", maxHeight: "90vh",
        display: "flex", flexDirection: "column",
        fontFamily: F,
      }}>

        {/* Header */}
        <div style={{
          display: "flex", alignItems: "center",
          justifyContent: "space-between",
          padding: "18px 20px 0", flexShrink: 0,
        }}>
          <div>
            <div style={{ fontSize: 16, fontWeight: 700, color: C.txt, display: "flex", alignItems: "center", gap: 7 }}>
              <IcStack size={17}/> Mes cartes
            </div>
            <div style={{ fontSize: 11, color: C.dim, marginTop: 2 }}>
              {user?.username} · {total} carte{total > 1 ? "s" : ""}
              {tab === "mine" && ` · ${publicCount}/${PUBLIC_LIMIT} partagées`}
            </div>
          </div>
          <button onClick={onClose} style={{
            background: "none", border: "none",
            color: C.dim, cursor: "pointer", display: "flex", padding: 2,
          }}><IcX size={20}/></button>
        </div>

        {/* Tabs */}
        <div style={{
          display: "flex", gap: 4, padding: "14px 20px 0",
          flexShrink: 0,
        }}>
          {tabs.map(t => (
            <button key={t.id} onClick={() => setTab(t.id)} style={{
              fontFamily: F, fontSize: 11, fontWeight: 500,
              padding: "6px 14px", borderRadius: 7, cursor: "pointer",
              background: tab === t.id ? C.acc + "22" : "transparent",
              color: tab === t.id ? C.acc : C.dim,
              border: `1px solid ${tab === t.id ? C.acc + "55" : C.bdr}`,
              transition: "all .15s",
              display: "inline-flex", alignItems: "center", gap: 5,
            }}>{t.icon && <t.icon size={13}/>} {t.label}</button>
          ))}

          {/* Recherche */}
          <div style={{ flex: 1, maxWidth: 260, marginLeft: "auto" }}>
            <input
              style={inp}
              placeholder="Rechercher…"
              value={search}
              onChange={e => setSearch(e.target.value)}
              onKeyDown={e => e.key === "Enter" && tab === "admin" && load(1)}
            />
          </div>
        </div>

        {/* Grille */}
        <div style={{
          flex: 1, overflowY: "auto",
          padding: "16px 20px 20px",
        }}>
          {loading && maps.length === 0 && (
            <div style={{
              display: "flex", alignItems: "center", justifyContent: "center",
              height: 200, fontSize: 12, color: C.dim,
            }}>Chargement…</div>
          )}

          {!loading && filtered.length === 0 && (
            <div style={{
              display: "flex", flexDirection: "column",
              alignItems: "center", justifyContent: "center",
              height: 200, gap: 10,
            }}>
              <div style={{ opacity: 0.2 }}><IcMap size={36}/></div>
              <div style={{ fontSize: 13, color: C.dim }}>
                {tab === "mine" ? "Aucune carte sauvegardée" : "Aucune carte publique"}
              </div>
              {tab === "mine" && (
                <div style={{ fontSize: 11, color: C.dim, display: "inline-flex", alignItems: "center", gap: 5 }}>
                  Cliquez sur <IcSave size={12}/> pour sauvegarder votre première carte
                </div>
              )}
            </div>
          )}

          <div style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))",
            gap: 16,
          }}>
            {filtered.map(m => (
              <MapCard
                key={m.id}
                map={m}
                C={C}
                isAdmin={tab === "admin"}
                onOpen={onOpenMap}
                onUpdate={onUpdateMap}
                onDelete={deleteMap}
                onTogglePublic={tab === "gallery" ? () => {} : togglePublic}
              />
            ))}
          </div>

          {/* Charger plus */}
          {maps.length < total && (
            <div style={{ textAlign: "center", marginTop: 20 }}>
              <button
                onClick={() => load(page + 1, false)}
                disabled={loading}
                style={{
                  fontFamily: F, fontSize: 12, padding: "8px 24px",
                  borderRadius: 8, cursor: loading ? "wait" : "pointer",
                  background: C.bg, color: C.dim, border: `1px solid ${C.bdr}`,
                }}
              >
                {loading ? "…" : `Charger plus (${total - maps.length} restantes)`}
              </button>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
