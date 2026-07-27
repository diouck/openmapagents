"""
canopy_routes.py — Produits foresterie LiDAR.

À partir d'un LAS/LAZ (toutes versions + COPC) :
  - MNT (sol), MNS (sursol), MNH = MNS − MNT (hauteur de canopée)
  - détection des cimes (maxima locaux du MNH)
  - délimitation des houppiers (watershed contrôlé par marqueurs)

Sorties : rasters PNG géoréférencés (overlay MapLibre), GeoTIFF téléchargeables,
GeoJSON (cimes + couronnes, en WGS84) et statistiques foresterie.

Endpoints :
  POST /api/lidar/canopy               → calcule tous les produits
  GET  /api/lidar/canopy/file/{job}/{name}.tif  → télécharge un GeoTIFF

Sol non classé : filtre CSF si le paquet `CSF` est présent, sinon repli
morphologique (ouverture en niveaux de gris du MNS) avec avertissement.

Déps : laspy lazrs pyproj numpy scipy scikit-image rasterio Pillow (CSF optionnel).
"""
import os
import io
import base64
import math
import time
import uuid
import tempfile
import glob
import shutil

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Response
from fastapi.responses import FileResponse

router = APIRouter(prefix="/api/lidar", tags=["lidar-canopy"])

_JOBS_DIR = os.path.join(tempfile.gettempdir(), "canopy_jobs")
_JOB_TTL = 3600            # s — durée de vie des GeoTIFF temporaires
_MAX_CELLS = 16_000_000    # plafond de grille (~4000×4000) → sinon on augmente le pas


def _cleanup_jobs():
    try:
        now = time.time()
        for d in glob.glob(os.path.join(_JOBS_DIR, "*")):
            if now - os.path.getmtime(d) > _JOB_TTL:
                shutil.rmtree(d, ignore_errors=True)
    except Exception:
        pass


# ── Rampes de couleur (256×3) ──────────────────────────────────────────────
def _ramp(anchors):
    import numpy as np
    xs = np.linspace(0, 1, len(anchors))
    t = np.linspace(0, 1, 256)
    a = np.asarray(anchors, dtype=float)
    out = np.zeros((256, 3), np.uint8)
    for c in range(3):
        out[:, c] = np.clip(np.interp(t, xs, a[:, c]), 0, 255).astype(np.uint8)
    return out


_CMAP_ELEV = _ramp([[68, 1, 84], [59, 82, 139], [33, 145, 140], [94, 201, 98], [253, 231, 37]])
_CMAP_CHM = _ramp([[247, 252, 245], [199, 233, 192], [116, 196, 118], [35, 139, 69], [0, 68, 27]])


def _hillshade(arr, res=1.0, z=1.0, az=315.0, alt=45.0):
    """Ombrage analytique d'un raster d'altitude (z = exagération verticale)."""
    import numpy as np
    dy, dx = np.gradient(np.nan_to_num(arr) * float(z), max(res, 1e-6))
    slope = np.pi / 2.0 - np.arctan(np.hypot(dx, dy))
    aspect = np.arctan2(-dx, dy)
    azr, altr = np.radians(360.0 - az + 90.0), np.radians(alt)
    hs = (np.sin(altr) * np.sin(slope) +
          np.cos(altr) * np.cos(slope) * np.cos(azr - aspect))
    return np.clip(hs, 0.0, 1.0)


def _colorize_png_b64(arr, mask, cmap, vmin, vmax, shade=None):
    """Colorise un raster (nodata transparent) → PNG base64.

    `shade` : ombrage 0-1 optionnel (exagération verticale) → modulation 0.5-1.1
    des teintes, ce qui « monte / baisse » visuellement le relief.
    """
    import numpy as np
    from PIL import Image
    h, w = arr.shape
    rgba = np.zeros((h, w, 4), np.uint8)
    span = (vmax - vmin) or 1.0
    norm = np.clip((arr - vmin) / span, 0, 1)
    idx = (norm * 255).astype(np.uint8)
    rgb = cmap[idx].astype(np.float32)
    if shade is not None:
        rgb = rgb * (shade[..., None] * 0.6 + 0.5)
    rgba[..., :3] = np.clip(rgb, 0, 255).astype(np.uint8)
    rgba[..., 3] = np.where(mask, 255, 0).astype(np.uint8)
    buf = io.BytesIO()
    Image.fromarray(rgba, "RGBA").save(buf, "PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _accum_grid(flat, idx, val, how):
    """Accumule val dans flat aux positions idx selon max/min (vectorisé)."""
    import numpy as np
    if idx.size == 0:
        return
    order = np.argsort(idx, kind="stable")
    idx_s = idx[order]
    v_s = val[order]
    uniq, first = np.unique(idx_s, return_index=True)
    if how == "max":
        m = np.maximum.reduceat(v_s, first)
        flat[uniq] = np.maximum(flat[uniq], m)
    else:
        m = np.minimum.reduceat(v_s, first)
        flat[uniq] = np.minimum(flat[uniq], m)


@router.post("/canopy")
async def canopy(
    file: UploadFile = File(None),
    file_token: str = Form(""),   # jeton d'un fichier déjà importé via /points (pas de ré-upload)
    resolution: float = Form(0.5),
    min_tree_height: float = Form(2.0),
    detect_trees: bool = Form(True),
    tree_min_distance: float = Form(2.0),
    aoi: str = Form(""),          # "minlon,minlat,maxlon,maxlat" (WGS84), optionnel
    z_factor: float = Form(1.0),  # exagération verticale de l'ombrage MNT/MNS/MNH (1 = neutre)
    sun_azimuth: float = Form(315.0),
    sun_altitude: float = Form(45.0),
):
    # Préchargement des dépendances foresterie → message clair si l'une manque
    # côté serveur (sinon 500 opaque « lors du calcul MNT »).
    try:
        import numpy as np
        import laspy
        from pyproj import Transformer
        from scipy import ndimage
        import rasterio
        from rasterio.transform import from_origin
        from rasterio.fill import fillnodata
        from rasterio.features import shapes as rio_shapes
        from skimage.feature import peak_local_max
        from skimage.segmentation import watershed
        from shapely.geometry import shape as _shp
    except ImportError as _e:
        raise HTTPException(
            503,
            f"Dépendance foresterie manquante côté serveur : « {getattr(_e, 'name', _e)} ». "
            f"Installez : pip install scipy scikit-image rasterio shapely — puis redémarrez le backend.",
        )

    _cleanup_jobs()
    t0 = time.time()
    warnings = []

    # ── Source : jeton d'un fichier déjà importé, sinon upload ──
    from lidar_routes import cached_path
    src_path = None
    owned = False                       # True → fichier temporaire à supprimer à la fin
    cached = cached_path(file_token) if file_token else None
    if cached:
        src_path = cached
    else:
        if file is None:
            raise HTTPException(422, "Aucun fichier ni jeton fourni (réimportez le LAS/LAZ).")
        head = await file.read(4)
        if head != b"LASF":
            raise HTTPException(422, "Fichier non LiDAR (signature LASF absente).")
        tf = tempfile.NamedTemporaryFile(suffix=".las", delete=False)
        src_path = tf.name
        owned = True
        tf.write(head)
        while True:
            buf = await file.read(1 << 20)
            if not buf:
                break
            tf.write(buf)
        tf.close()

    try:
        try:
            reader = laspy.open(src_path)
        except Exception as e:
            raise HTTPException(422, f"Lecture LAS/LAZ échouée : {e}")

        hdr = reader.header
        n_total = int(hdr.point_count)
        if n_total == 0:
            raise HTTPException(422, "Nuage vide.")
        try:
            dims = set(hdr.point_format.dimension_names)
        except Exception:
            dims = set()
        has_cls = "classification" in dims

        crs = None
        try:
            crs = hdr.parse_crs()
        except Exception:
            crs = None
        if crs is None:
            raise HTTPException(422, "CRS absent du fichier : impossible de géoréférencer les produits.")
        if getattr(crs, "is_geographic", False):
            raise HTTPException(422, "CRS géographique (degrés) non supporté pour la rastérisation : fournissez un LAS en projection métrique (UTM, Lambert-93…).")

        # ── Emprise (AOI éventuelle) ───────────────────────
        xmin, ymin = float(hdr.mins[0]), float(hdr.mins[1])
        xmax, ymax = float(hdr.maxs[0]), float(hdr.maxs[1])
        if aoi.strip():
            try:
                a = [float(v) for v in aoi.split(",")]
                to_src = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
                xs, ys = to_src.transform([a[0], a[2], a[0], a[2]], [a[1], a[3], a[3], a[1]])
                axmin, axmax = min(xs), max(xs)
                aymin, aymax = min(ys), max(ys)
                xmin, xmax = max(xmin, axmin), min(xmax, axmax)
                ymin, ymax = max(ymin, aymin), min(ymax, aymax)
                if xmax <= xmin or ymax <= ymin:
                    raise HTTPException(422, "La zone dessinée ne recouvre pas la dalle.")
            except HTTPException:
                raise
            except Exception:
                warnings.append("AOI ignorée (format attendu : minlon,minlat,maxlon,maxlat).")

        # ── Résolution / grille (plafond mémoire) ──────────
        res = max(0.1, float(resolution))
        W = max(1, int(math.ceil((xmax - xmin) / res)))
        H = max(1, int(math.ceil((ymax - ymin) / res)))
        if W * H > _MAX_CELLS:
            factor = math.sqrt(W * H / _MAX_CELLS)
            res *= factor
            W = max(1, int(math.ceil((xmax - xmin) / res)))
            H = max(1, int(math.ceil((ymax - ymin) / res)))
            warnings.append(f"Résolution ramenée à {res:.2f} m (emprise trop grande).")

        dsm = np.full(W * H, -np.inf, dtype=np.float64)      # MNS : Z max
        grd = np.full(W * H, np.inf, dtype=np.float64)       # sol (classe 2) : Z min
        veg = np.full(W * H, -np.inf, dtype=np.float64)      # végétation (cl. 3/4/5) : Z max
        bld = np.full(W * H, -np.inf, dtype=np.float64)      # bâtiment (cl. 6) : Z max
        allmin = np.full(W * H, np.inf, dtype=np.float64)    # Z min (repli sol)
        ground_pts = 0
        n_read = 0

        # Échantillon décimé (pour CSF / stats si non classé)
        keep_cap = 3_000_000
        stride = max(1, n_total // keep_cap)
        sx, sy, sz = [], [], []

        with reader:
            for pts in reader.chunk_iterator(1_000_000):
                x = np.asarray(pts.x, dtype=np.float64)
                y = np.asarray(pts.y, dtype=np.float64)
                z = np.asarray(pts.z, dtype=np.float64)
                m = (x >= xmin) & (x < xmax) & (y >= ymin) & (y < ymax)
                if not m.any():
                    continue
                x, y, z = x[m], y[m], z[m]
                col = np.clip(((x - xmin) / res).astype(np.int64), 0, W - 1)
                row = np.clip(((ymax - y) / res).astype(np.int64), 0, H - 1)
                idx = row * W + col
                _accum_grid(dsm, idx, z, "max")
                _accum_grid(allmin, idx, z, "min")
                if has_cls:
                    cls = np.asarray(pts.classification, dtype=np.uint8)[m]
                    gm = cls == 2
                    if gm.any():
                        ground_pts += int(gm.sum())
                        _accum_grid(grd, idx[gm], z[gm], "min")
                    vm = (cls == 3) | (cls == 4) | (cls == 5)     # végétation basse/moy/haute
                    if vm.any():
                        _accum_grid(veg, idx[vm], z[vm], "max")
                    bm = cls == 6                                  # bâtiment
                    if bm.any():
                        _accum_grid(bld, idx[bm], z[bm], "max")
                # échantillon décimé
                if x.size:
                    sel = slice(0, None, stride)
                    sx.append(x[sel]); sy.append(y[sel]); sz.append(z[sel])
                n_read += int(x.size)

        if n_read == 0:
            raise HTTPException(422, "Aucun point dans l'emprise.")

        dsm2 = dsm.reshape(H, W)
        dsm_mask = np.isfinite(dsm2)
        # MNS : remplissage des trous par IDW
        dsm_fill = dsm2.copy()
        dsm_fill[~dsm_mask] = -9999.0
        dsm_fill = fillnodata(dsm_fill.astype(np.float32), mask=dsm_mask.astype(np.uint8),
                              max_search_distance=50.0)

        # ── MNT : sol classé, sinon CSF, sinon repli morphologique ──
        ground_mode = None
        gnd2 = grd.reshape(H, W)
        gnd_mask = np.isfinite(gnd2)
        if has_cls and ground_pts > 50 and gnd_mask.sum() > 20:
            ground_mode = "classification (classe 2)"
            dtm_seed = gnd2.copy()
            seed_mask = gnd_mask
        else:
            sxa = np.concatenate(sx) if sx else np.array([])
            sya = np.concatenate(sy) if sy else np.array([])
            sza = np.concatenate(sz) if sz else np.array([])
            gpts = _csf_ground(sxa, sya, sza, res)
            if gpts is not None and gpts[0].size > 50:
                ground_mode = "filtre CSF (nuage non classé)"
                gx, gy, gz = gpts
                gcol = np.clip(((gx - xmin) / res).astype(np.int64), 0, W - 1)
                grow = np.clip(((ymax - gy) / res).astype(np.int64), 0, H - 1)
                gflat = np.full(W * H, np.inf, dtype=np.float64)
                _accum_grid(gflat, grow * W + gcol, gz, "min")
                dtm_seed = gflat.reshape(H, W)
                seed_mask = np.isfinite(dtm_seed)
            else:
                ground_mode = "repli morphologique (sans classe sol ni CSF)"
                warnings.append("Sol estimé par ouverture morphologique du MNS (moins précis) : "
                                "classez le sol (classe 2) ou installez le paquet CSF pour un vrai MNT.")
                base = allmin.reshape(H, W).copy()
                base[~np.isfinite(base)] = np.nanmax(dsm_fill)
                rad = max(1, int(round(15.0 / res)))     # ~15 m
                opened = ndimage.grey_opening(base, size=(rad, rad))
                dtm_seed = opened
                seed_mask = np.ones_like(opened, dtype=bool)

        dtm_fill = dtm_seed.astype(np.float32).copy()
        dtm_fill[~seed_mask] = -9999.0
        dtm_fill = fillnodata(dtm_fill, mask=seed_mask.astype(np.uint8), max_search_distance=200.0)
        # léger lissage du MNT
        dtm_fill = ndimage.median_filter(dtm_fill, size=3)

        # ── MNH (hauteur normalisée : tous objets, bâti inclus) ─
        chm = (dsm_fill - dtm_fill)
        chm[~dsm_mask] = 0.0
        chm = np.where(np.isfinite(chm), chm, 0.0)
        chm = np.clip(chm, 0, None)

        # ── Canopée pour la DÉTECTION (végétation uniquement) ──
        # On exclut le bâti : sinon toits/pignons sont détectés comme « arbres ».
        # Si classif présente → hauteur des points végétation (cl. 3/4/5) au-dessus
        # du MNT ; sinon on retombe sur le MNH complet (bâti possible → avertissement).
        veg2 = veg.reshape(H, W)
        veg_mask = np.isfinite(veg2)
        bld2 = bld.reshape(H, W)
        bld_mask = np.isfinite(bld2)
        if has_cls and veg_mask.sum() > 20:
            chm_trees = np.zeros((H, W), dtype=np.float32)
            chm_trees[veg_mask] = np.clip((veg2[veg_mask] - dtm_fill[veg_mask]).astype(np.float32), 0, None)
            # Retrait EXPLICITE du bâti : la canopée de détection est mise à 0 sur
            # l'emprise des bâtiments (classe 6) → aucun toit/pignon en « arbre ».
            chm_trees[bld_mask] = 0.0
            canopy_from = "végétation (cl. 3/4/5), bâti (cl. 6) exclu"
        else:
            chm_trees = chm.astype(np.float32)
            canopy_from = "MNH complet (bâti possiblement inclus)"
            if detect_trees:
                warnings.append("Pas de classe végétation : la détection porte sur le MNH complet ; "
                                "des bâtiments peuvent être comptés comme arbres.")
        chm_s = ndimage.gaussian_filter(chm_trees, sigma=1.0)

        # ── Détection des cimes + houppiers ────────────────
        treetops = {"type": "FeatureCollection", "features": []}
        crowns = {"type": "FeatureCollection", "features": []}
        n_trees = 0
        crown_areas = []
        tree_heights = []
        if detect_trees:
            min_dist = max(1, int(round(float(tree_min_distance) / res)))
            peaks = peak_local_max(chm_s, min_distance=min_dist,
                                   threshold_abs=float(min_tree_height),
                                   exclude_border=False)
            n_trees = int(peaks.shape[0])
            to_ll = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
            if n_trees > 0:
                markers = np.zeros(chm_s.shape, dtype=np.int32)
                for i, (r, c) in enumerate(peaks, start=1):
                    markers[r, c] = i
                mask_veg = chm_s > float(min_tree_height)
                labels = watershed(-chm_s, markers, mask=mask_veg)

                # stats par houppier (sur la canopée végétation)
                ids = np.arange(1, n_trees + 1)
                hmax = ndimage.maximum(chm_trees, labels, ids)
                hmean = ndimage.mean(chm_trees, labels, ids)
                hmin = ndimage.minimum(chm_trees, labels, ids)
                px = np.bincount(labels.ravel(), minlength=n_trees + 1)[1:]
                area_by_id = px * (res * res)
                # diamètre de couronne (cercle équivalent) par houppier
                diam_by_id = 2.0 * np.sqrt(np.maximum(area_by_id, 0) / math.pi)

                # cimes → points WGS84 (avec métriques)
                for i, (r, c) in enumerate(peaks, start=1):
                    xw = xmin + (c + 0.5) * res
                    yw = ymax - (r + 0.5) * res
                    lon, lat = to_ll.transform(xw, yw)
                    h = float(chm_trees[r, c])
                    tree_heights.append(h)
                    treetops["features"].append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [round(lon, 7), round(lat, 7)]},
                        "properties": {
                            "id": i,
                            "height_m": round(h, 2),
                            "crown_id": i,
                            "crown_area_m2": round(float(area_by_id[i - 1]), 1),
                            "crown_diam_m": round(float(diam_by_id[i - 1]), 2),
                            "lon": round(lon, 7),
                            "lat": round(lat, 7),
                        },
                    })

                # houppiers → polygones WGS84 (avec métriques)
                transform = from_origin(xmin, ymax, res, res)
                for geom, val in rio_shapes(labels.astype(np.int32),
                                            mask=(labels > 0), transform=transform):
                    lid = int(val)
                    if lid < 1 or lid > n_trees:
                        continue
                    perim = float(_shp(geom).length)          # périmètre en CRS source (m)
                    ll_rings = []
                    for ring in geom["coordinates"]:
                        ll_rings.append([list(to_ll.transform(px_, py_)) for px_, py_ in ring])
                    a_m2 = float(area_by_id[lid - 1])
                    hmax_ = float(hmax[lid - 1]); hmean_ = float(hmean[lid - 1]); hmin_ = float(hmin[lid - 1])
                    compact = (4.0 * math.pi * a_m2 / (perim * perim)) if perim > 0 else 0.0
                    crown_areas.append(a_m2)
                    crowns["features"].append({
                        "type": "Feature",
                        "geometry": {"type": "Polygon", "coordinates": ll_rings},
                        "properties": {
                            "id": lid,
                            "area_m2": round(a_m2, 1),
                            "perimeter_m": round(perim, 1),
                            "crown_diam_m": round(float(diam_by_id[lid - 1]), 2),
                            "height_max_m": round(hmax_, 2),
                            "height_mean_m": round(hmean_, 2),
                            "height_min_m": round(hmin_, 2),
                            "volume_m3": round(a_m2 * hmean_, 1),   # approx (surface × hauteur moyenne)
                            "compactness": round(compact, 3),        # 4πA/P² (1 = cercle)
                        },
                    })

        # ── Emprise en lon/lat (4 coins pour overlay image) ─
        to_ll = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
        def _ll(px_, py_):
            lo, la = to_ll.transform(px_, py_)
            return [lo, la]
        corners = [_ll(xmin, ymax), _ll(xmax, ymax), _ll(xmax, ymin), _ll(xmin, ymin)]  # TL,TR,BR,BL

        # ── Rasters PNG ────────────────────────────────────
        elev_valid = dsm_fill[dsm_mask]
        e_lo = float(np.percentile(elev_valid, 2)) if elev_valid.size else 0.0
        e_hi = float(np.percentile(elev_valid, 98)) if elev_valid.size else 1.0
        # Le MNH livré = canopée VÉGÉTATION uniquement (bâti exclu).
        chm_hi = float(np.percentile(chm_trees[chm_trees > 0.5], 98)) if np.any(chm_trees > 0.5) else 1.0
        # Ombrage (exagération verticale) appliqué aux 3 rasters si z_factor > 1
        _zf = float(z_factor or 1.0)
        _sh_dtm = _hillshade(dtm_fill, res, _zf, sun_azimuth, sun_altitude) if _zf > 1.0 else None
        _sh_dsm = _hillshade(dsm_fill, res, _zf, sun_azimuth, sun_altitude) if _zf > 1.0 else None
        _sh_chm = _hillshade(chm_trees, res, _zf, sun_azimuth, sun_altitude) if _zf > 1.0 else None
        dtm_png = _colorize_png_b64(dtm_fill, np.ones_like(dtm_fill, bool), _CMAP_ELEV, e_lo, e_hi, _sh_dtm)
        dsm_png = _colorize_png_b64(dsm_fill, dsm_mask, _CMAP_ELEV, e_lo, e_hi, _sh_dsm)
        chm_png = _colorize_png_b64(chm_trees, chm_trees > 0.5, _CMAP_CHM, 0.0, max(chm_hi, 1.0), _sh_chm)

        # ── GeoTIFF (job temporaire) ───────────────────────
        job = uuid.uuid4().hex[:12]
        jdir = os.path.join(_JOBS_DIR, job)
        os.makedirs(jdir, exist_ok=True)
        transform = from_origin(xmin, ymax, res, res)
        crs_wkt = crs.to_wkt()

        def _tif(name, arr, mask=None):
            path = os.path.join(jdir, name)
            data = arr.astype(np.float32).copy()
            if mask is not None:
                data[~mask] = -9999.0
            with rasterio.open(path, "w", driver="GTiff", height=H, width=W, count=1,
                               dtype="float32", crs=crs_wkt, transform=transform,
                               nodata=-9999.0, compress="deflate") as dst:
                dst.write(data, 1)
            return f"/api/lidar/canopy/file/{job}/{name}"

        downloads = {
            "mnt": _tif("mnt.tif", dtm_fill),
            "mns": _tif("mns.tif", dsm_fill, dsm_mask),
            "mnh": _tif("mnh.tif", chm_trees, chm_trees > 0.05),   # canopée végétation
        }

        # ── Statistiques ───────────────────────────────────
        area_ha = ((xmax - xmin) * (ymax - ymin)) / 10000.0
        cover_cells = int(np.count_nonzero(chm_trees > float(min_tree_height)))
        valid_cells = int(np.count_nonzero(dsm_mask))
        th = np.asarray(tree_heights, dtype=float)
        stats = {
            "area_ha": round(area_ha, 3),
            "resolution_m": round(res, 3),
            "n_points_used": int(n_read),
            "ground_mode": ground_mode,
            "canopy_from": canopy_from,
            "n_trees": int(n_trees),
            "trees_per_ha": round(n_trees / area_ha, 1) if area_ha > 0 else None,
            "canopy_cover_pct": round(100.0 * cover_cells / valid_cells, 1) if valid_cells else 0.0,
            "height_mean_m": round(float(th.mean()), 2) if th.size else None,
            "height_max_m": round(float(th.max()), 2) if th.size else None,
            "height_p95_m": round(float(np.percentile(th, 95)), 2) if th.size else None,
            "crown_area_mean_m2": round(float(np.mean(crown_areas)), 1) if crown_areas else None,
        }

        print(f"[canopy] {n_read} pts · grille {W}x{H}@{res:.2f}m · sol={ground_mode} · "
              f"{n_trees} arbres · {time.time() - t0:.1f}s")

        return {
            "job": job,                          # → tuiles terrain-RGB /api/lidar/dem/{job}/…
            "crs": crs.name,
            "resolution_m": round(res, 3),
            "grid": {"w": W, "h": H},
            "image_coordinates": corners,       # TL,TR,BR,BL (lon,lat) pour MapLibre
            "bbox_lonlat": [corners[0][0], corners[2][1], corners[1][0], corners[0][1]],  # w,s,e,n

            "chm_max_m": round(float(max(chm_hi, 1.0)), 2),
            "elev_min_m": round(e_lo, 1),
            "elev_max_m": round(e_hi, 1),
            "png": {"mnt": dtm_png, "mns": dsm_png, "mnh": chm_png},
            "downloads": downloads,
            "treetops": treetops,
            "crowns": crowns,
            "stats": stats,
            "warnings": warnings,
        }
    except HTTPException:
        raise
    except Exception as _e:
        # Trace complète dans les logs serveur + message lisible côté client
        # (le 500 opaque devient actionnable dans l'UI).
        import traceback
        print("[canopy] ERREUR 500 :\n" + traceback.format_exc())
        raise HTTPException(500, f"Échec du calcul foresterie : {type(_e).__name__}: {_e}")
    finally:
        # On ne supprime que le fichier temporaire qu'on a créé (upload direct) ;
        # le fichier caché (jeton) reste géré par lidar_routes (TTL).
        if owned and src_path and os.path.exists(src_path):
            try:
                os.remove(src_path)
            except Exception:
                pass


def _csf_ground(x, y, z, res):
    """Filtre CSF sur un échantillon (x,y,z) → (gx,gy,gz) points sol, ou None."""
    try:
        import CSF
        import numpy as np
    except Exception:
        return None
    if x.size < 100:
        return None
    try:
        csf = CSF.CSF()
        csf.params.cloth_resolution = max(0.5, res * 2)
        csf.params.bSloopSmooth = True
        csf.params.rigidness = 2
        pts = np.column_stack([x, y, z]).astype(np.float64).tolist()
        csf.setPointCloud(pts)
        gi = CSF.VecInt()
        ng = CSF.VecInt()
        csf.do_filtering(gi, ng)
        g = np.asarray(gi, dtype=np.int64)
        if g.size == 0:
            return None
        return x[g], y[g], z[g]
    except Exception:
        return None


@router.get("/canopy/file/{job}/{name}")
def canopy_file(job: str, name: str):
    # garde-fou anti-traversal
    if not job.isalnum() or not name.replace(".", "").replace("_", "").isalnum():
        raise HTTPException(400, "Nom invalide.")
    path = os.path.join(_JOBS_DIR, job, name)
    if not os.path.isfile(path):
        raise HTTPException(404, "Fichier expiré ou introuvable (régénérez le produit).")
    return FileResponse(path, media_type="image/tiff", filename=name)


# ── Tuiles terrain-RGB (Terrarium) : le MNT/MNS/MNH devient le TERRAIN 3D ──────
# Un overlay image est plat ; pour que les bâtiments (MNS) se lèvent réellement,
# MapLibre a besoin d'une source `raster-dem`. On sert donc le GeoTIFF du job
# reprojeté en Web Mercator, encodé Terrarium : h = R*256 + G + B/256 − 32768.
_WM_R = 20037508.342789244


@router.get("/dem/{job}/{z}/{x}/{y}.png")
def lidar_dem_tile(job: str, z: int, x: int, y: int, product: str = "mns"):
    try:
        import numpy as np
        import rasterio
        from rasterio.warp import reproject
        from rasterio.transform import from_bounds as _tf_from_bounds
        from rasterio.enums import Resampling
        from PIL import Image
    except ImportError as _e:
        raise HTTPException(503, f"Dépendance manquante : {getattr(_e, 'name', _e)}")

    if not job.isalnum() or product not in ("mnt", "mns", "mnh"):
        raise HTTPException(400, "Paramètres invalides.")
    path = os.path.join(_JOBS_DIR, job, f"{product}.tif")
    if not os.path.isfile(path):
        raise HTTPException(404, "Job expiré — relancez le calcul.")

    n = 2 ** int(z)
    if not (0 <= x < n and 0 <= y < n):
        raise HTTPException(400, "Tuile hors domaine.")
    size = 2.0 * _WM_R / n
    minx = -_WM_R + x * size
    maxx = minx + size
    maxy = _WM_R - y * size
    miny = maxy - size
    TS = 256

    # Reprojection DIRECTE dans la grille de la tuile (WarpedVRT interdit les
    # lectures « boundless » → 500 sur chaque tuile ; testé sur un MNS réel).
    try:
        dst = np.full((TS, TS), -9999.0, dtype="float32")
        dst_tf = _tf_from_bounds(minx, miny, maxx, maxy, TS, TS)
        with rasterio.open(path) as src:
            reproject(source=rasterio.band(src, 1), destination=dst,
                      src_transform=src.transform, src_crs=src.crs, src_nodata=src.nodata,
                      dst_transform=dst_tf, dst_crs="EPSG:3857", dst_nodata=-9999.0,
                      resampling=Resampling.bilinear)
    except Exception as e:
        raise HTTPException(500, f"Lecture tuile DEM : {e}")

    a = np.asarray(dst, dtype=np.float64)
    a[~np.isfinite(a)] = -9999.0
    a[a <= -9998.0] = 0.0                      # nodata → altitude 0 (hors emprise)
    v = np.clip(a + 32768.0, 0.0, 65535.999)   # encodage Terrarium
    r = np.floor(v / 256.0).astype(np.uint8)
    g = np.floor(v % 256.0).astype(np.uint8)
    b = np.floor((v - np.floor(v)) * 256.0).astype(np.uint8)
    buf = io.BytesIO()
    Image.fromarray(np.dstack([r, g, b]), "RGB").save(buf, "PNG")
    return Response(content=buf.getvalue(), media_type="image/png",
                    headers={"Cache-Control": "public, max-age=3600"})
