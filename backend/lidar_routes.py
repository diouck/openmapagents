"""
lidar_routes.py — Conversion LAS/LAZ côté serveur via laspy.

Le lecteur navigateur (loaders.gl) ne lit que LAS ≤ 1.3 et pas tous les LAZ.
Ici on lit **toutes** les versions LAS 1.0–1.4 + LAZ (backend lazrs), on
sous-échantillonne, on recentre les points (offsets mètres autour du centroïde)
et on renvoie positions/couleurs en base64 (Float32 / Uint8) pour alimenter un
deck.gl PointCloudLayer côté client.

Installation serveur :
    pip install laspy lazrs numpy python-multipart
Enregistrement (agent.py) :
    from lidar_routes import router as lidar_router
    app.include_router(lidar_router)
"""
import io
import os
import time
import uuid
import glob
import shutil
import tempfile
import base64

from fastapi import APIRouter, UploadFile, File, HTTPException, Query

router = APIRouter(prefix="/api/lidar", tags=["lidar"])

# ── Cache serveur des fichiers importés ─────────────────────────────────────
# /points y range le LAS/LAZ reçu et renvoie un jeton ; /canopy (foresterie)
# réutilise ce jeton → pas de ré-upload du fichier déjà envoyé.
LIDAR_CACHE = os.path.join(tempfile.gettempdir(), "lidar_cache")
_CACHE_TTL = 3600  # s


def _cache_cleanup():
    try:
        now = time.time()
        for p in glob.glob(os.path.join(LIDAR_CACHE, "*")):
            if now - os.path.getmtime(p) > _CACHE_TTL:
                os.remove(p)
    except Exception:
        pass


def cache_move(src_path):
    """Déplace un fichier temporaire dans le cache et renvoie son jeton."""
    _cache_cleanup()
    os.makedirs(LIDAR_CACHE, exist_ok=True)
    token = uuid.uuid4().hex[:16]
    dst = os.path.join(LIDAR_CACHE, token + ".las")
    shutil.move(src_path, dst)
    return token


def cached_path(token):
    """Chemin du fichier caché pour un jeton, ou None (expiré/invalide)."""
    if not token or not str(token).isalnum():
        return None
    p = os.path.join(LIDAR_CACHE, str(token) + ".las")
    return p if os.path.isfile(p) else None

MAX_POINTS = 500_000


def _copc_sample(path, target, cap):
    """Échantillon spatialement uniforme d'un fichier **COPC** via son octree.

    Un COPC (Cloud Optimized Point Cloud) range ses points en octree, PAS de
    façon séquentielle : l'échantillonnage par ``seek()`` y récupère donc des
    points spatialement **agglutinés** (2 % de l'emprise) et parfois des points
    parasites à (0,0) — qui, en Lambert-93, se reprojettent dans l'Atlantique
    et entraînent tout le nuage « dans l'océan ».

    On interroge plutôt l'octree par niveaux croissants jusqu'à obtenir ~``target``
    points : couverture spatiale **complète et uniforme**, en ne lisant que
    l'aperçu (très rapide). Renvoie un point record laspy, ou ``None`` si le
    fichier n'est pas un COPC (→ repli sur l'échantillonnage seek).
    """
    try:
        import numpy as np
        from laspy.copc import CopcReader
    except Exception:
        return None
    try:
        cr = CopcReader.open(path)
    except Exception:
        return None                       # pas un COPC → repli seek
    try:
        with cr:
            pts = None
            for lvl in range(0, 16):
                pts = cr.query(level=lvl)
                if pts is not None and len(pts) >= target:
                    break
            if pts is None or len(pts) == 0:
                return None
            if len(pts) > cap:            # niveau trop dense → décimation régulière
                step = int(np.ceil(len(pts) / cap))
                pts = pts[::step]
            return pts
    except Exception:
        return None


@router.get("/health")
def lidar_health():
    try:
        import laspy  # noqa: F401
        import numpy   # noqa: F401
        ok_laz = True
        try:
            import lazrs  # noqa: F401
        except Exception:
            ok_laz = False
        # pyproj = géoréférencement des nuages projetés (UTM, Lambert-93…).
        # Absent → placement au centre de la carte au lieu du bon endroit.
        ok_proj = True
        try:
            import pyproj  # noqa: F401
        except Exception:
            ok_proj = False
        return {"status": "ok", "ready": True, "laz": ok_laz, "pyproj": ok_proj}
    except Exception as e:
        return {"status": "error", "ready": False, "message": str(e)}


@router.post("/points")
async def lidar_points(
    file: UploadFile = File(...),
    max_points: int = Query(MAX_POINTS, ge=1000, le=3_000_000),
):
    """Lit un fichier LAS/LAZ et renvoie un nuage de points recentré.

    Réponse JSON :
      - ``count``         : nombre de points renvoyés (après sous-échantillonnage)
      - ``total``         : nombre de points du fichier
      - ``positions_b64`` : Float32 little-endian [x,y,z,...] recentrés (base64)
      - ``colors_b64``    : Uint8 [r,g,b,...] 0-255 (base64) ou null
      - ``center``        : [cx, cy, cz] centroïde retiré (coordonnées source)
      - ``bounds``        : [xmin,ymin,zmin,xmax,ymax,zmax]
      - ``version``       : version LAS (ex. "1.4")
    """
    import tempfile, os, math, time
    _t0 = time.time()
    print(f"[lidar] /points reçu (échantillonnage seek v2) — fichier: {getattr(file, 'filename', '?')}")

    head = await file.read(4)
    if not head or len(head) < 4:
        raise HTTPException(422, "Fichier vide ou illisible.")
    # LiDAR LAS/LAZ commence TOUJOURS par 'LASF'. Sinon → log ASCII (forage), etc.
    if head != b"LASF":
        raise HTTPException(
            422,
            "Ce fichier .las n'est pas un nuage de points LiDAR (signature « LASF » "
            "absente). C'est probablement un log ASCII (forage/puits) qui partage la "
            "même extension. Fournissez un vrai fichier LiDAR LAS/LAZ.",
        )

    try:
        import numpy as np
        import laspy
    except ImportError as e:
        raise HTTPException(
            503,
            f"Dépendance manquante côté serveur : {e}. Installez : pip install laspy lazrs numpy.",
        )

    # ── Streaming de l'upload vers un fichier temporaire (mémoire bornée) ──
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".las", delete=False) as tf:
            tmp_path = tf.name
            tf.write(head)
            while True:
                buf = await file.read(1 << 20)   # 1 Mo par bloc
                if not buf:
                    break
                tf.write(buf)

        _t_read = time.time()
        print(f"[lidar] upload reçu en {_t_read - _t0:.1f}s "
              f"({os.path.getsize(tmp_path) / 1048576:.1f} Mo)")

        try:
            reader = laspy.open(tmp_path)
        except Exception as e:
            msg = str(e)
            if "laz" in msg.lower() or "compress" in msg.lower():
                raise HTTPException(422, f"Décompression LAZ impossible ({msg}). Installez : pip install lazrs.")
            raise HTTPException(422, f"Lecture LAS/LAZ échouée : {msg}")

        with reader:
            hdr = reader.header
            n_total = int(hdr.point_count)
            if n_total == 0:
                raise HTTPException(422, "Nuage de points vide.")

            try:
                dims = set(hdr.point_format.dimension_names)
            except Exception:
                dims = set()
            has_rgb = {"red", "green", "blue"} <= dims

            target = min(int(max_points), n_total)

            # ── Échantillonnage ─────────────────────────────────────────────
            # 1) COPC (LAZ en octree) → requête par niveau d'octree : couverture
            #    spatiale COMPLÈTE et uniforme, très rapide (lit l'aperçu). Le
            #    seek est inadapté au COPC (points en octree → cluster + points
            #    parasites (0,0) qui envoyaient le nuage « dans l'océan »).
            # 2) LAS/LAZ classique → sauts seek répartis (rapide, emprise complète).
            records = []
            sample_mode = "full"
            if n_total <= target:
                records.append(reader.read_points(n_total))
            else:
                copc_pts = _copc_sample(tmp_path, target, int(max_points))
                if copc_pts is not None:
                    records.append(copc_pts)
                    sample_mode = "copc"
                else:
                    sample_mode = "seek"
                    n_batches = min(target, 150)
                    per = max(1, target // n_batches)
                    for i in range(n_batches):
                        start = min((i * n_total) // n_batches, n_total - per)
                        reader.seek(max(0, start))
                        rec = reader.read_points(per)
                        if len(rec.x) > 0:
                            records.append(rec)

            if not records:
                raise HTTPException(422, "Nuage de points vide après échantillonnage.")

            x = np.concatenate([np.asarray(r.x, dtype=np.float64) for r in records])
            y = np.concatenate([np.asarray(r.y, dtype=np.float64) for r in records])
            z = np.concatenate([np.asarray(r.z, dtype=np.float64) for r in records])
            count = int(x.size)
            cx, cy, cz = float(x.mean()), float(y.mean()), float(z.mean())

            # ── Reprojection PAR POINT en lon/lat, puis offsets en vrais ──
            # mètres est/nord. Placement correct quelle que soit la projection
            # source (UTM, Lambert-93…) — pas d'erreur de convergence des
            # méridiens (qui faisait « tourner » les nuages Lambert-93).
            crs = None
            georef_error = None            # raison lisible si le géoréférencement échoue
            try:
                crs = hdr.parse_crs()
            except Exception as e:
                crs = None
                georef_error = f"CRS illisible dans l'en-tête ({e})"
            center_lonlat = None
            if crs is not None:
                try:
                    if getattr(crs, "is_geographic", False):
                        lon = np.asarray(x, dtype=np.float64)
                        lat = np.asarray(y, dtype=np.float64)
                    else:
                        from pyproj import Transformer
                        tr = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
                        lon, lat = tr.transform(x, y)
                        lon = np.asarray(lon, dtype=np.float64)
                        lat = np.asarray(lat, dtype=np.float64)
                    lon0 = float(np.nanmean(lon)); lat0 = float(np.nanmean(lat))
                    # Garde-fou : une reprojection ratée renvoie souvent inf/nan ou
                    # (0,0) → le nuage tombait « dans l'océan ». On refuse ces cas.
                    if not (math.isfinite(lon0) and math.isfinite(lat0)
                            and -180.0 <= lon0 <= 180.0 and -90.0 <= lat0 <= 90.0):
                        raise ValueError(f"coordonnées reprojetées invalides (lon={lon0}, lat={lat0})")
                    center_lonlat = [lon0, lat0]
                    clat = math.cos(math.radians(lat0))
                    ox = (lon - lon0) * 111320.0 * clat
                    oy = (lat - lat0) * 110540.0
                except ImportError:
                    georef_error = "pyproj absent du serveur (pip install pyproj)"
                    center_lonlat = None
                    ox = (x - cx); oy = (y - cy)
                except Exception as e:
                    georef_error = f"reprojection vers WGS84 échouée ({e})"
                    center_lonlat = None
                    ox = (x - cx); oy = (y - cy)
            else:
                if georef_error is None:
                    georef_error = "aucun système de coordonnées (CRS) dans le fichier"
                ox = (x - cx); oy = (y - cy)

            pos = np.empty(count * 3, dtype=np.float32)
            pos[0::3] = ox.astype(np.float32)
            pos[1::3] = oy.astype(np.float32)
            pos[2::3] = (z - cz).astype(np.float32)

            # ── Couleurs RGB ──
            colors_b64 = None
            if has_rgb:
                try:
                    r = np.concatenate([np.asarray(rr.red) for rr in records])
                    g = np.concatenate([np.asarray(rr.green) for rr in records])
                    b = np.concatenate([np.asarray(rr.blue) for rr in records])
                    rgb_mx = int(max(int(r.max()) if r.size else 0,
                                     int(g.max()) if g.size else 0,
                                     int(b.max()) if b.size else 0))
                    scale = (255.0 / 65535.0) if rgb_mx > 255 else 1.0
                    col = np.empty(count * 3, dtype=np.uint8)
                    col[0::3] = np.clip(r * scale, 0, 255).astype(np.uint8)
                    col[1::3] = np.clip(g * scale, 0, 255).astype(np.uint8)
                    col[2::3] = np.clip(b * scale, 0, 255).astype(np.uint8)
                    colors_b64 = base64.b64encode(col.tobytes()).decode("ascii")
                except Exception:
                    colors_b64 = None

            # ── Classification ASPRS ──
            classification_b64 = None
            class_histogram = {}
            try:
                cls = np.concatenate([np.asarray(rr.classification).astype(np.uint8) for rr in records])
                if cls.size == count:
                    classification_b64 = base64.b64encode(cls.tobytes()).decode("ascii")
                    vals, counts = np.unique(cls, return_counts=True)
                    class_histogram = {int(v_): int(c_) for v_, c_ in zip(vals, counts)}
            except Exception:
                pass

            v = hdr.version
            print(f"[lidar] {n_total} pts -> {count} echantillonnes ({sample_mode}) · "
                  f"traitement {time.time() - _t_read:.1f}s · total {time.time() - _t0:.1f}s · "
                  f"CRS={crs.name if crs is not None else None} · "
                  f"geo={'ok' if center_lonlat else georef_error}")
            result = {
                "count": count,
                "total": n_total,
                "positions_b64": base64.b64encode(pos.tobytes()).decode("ascii"),
                "colors_b64": colors_b64,
                "classification_b64": classification_b64,
                "class_histogram": class_histogram,
                "center": [cx, cy, cz],
                "center_lonlat": center_lonlat,
                "georef_error": georef_error,
                "bounds": [float(x.min()), float(y.min()), float(z.min()),
                           float(x.max()), float(y.max()), float(z.max())],
                "version": f"{v.major}.{v.minor}",
                "crs": (crs.name if crs is not None else None),
            }
        # ── Fichier fermé → mise en cache pour la foresterie (pas de ré-upload) ──
        try:
            _tok = cache_move(tmp_path)
            tmp_path = None
            result["file_token"] = _tok
        except Exception:
            result["file_token"] = None
        return result
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
