"""
raster_routes.py — Import + restyle de rasters GeoTIFF pour affichage carte.

POST /api/raster/import   (upload .tif/.tiff) → reprojette en EPSG:4326, met la
    bande en cache (mono-bande) et renvoie un PNG géoréférencé + 4 coins lon/lat
    + bbox + statistiques (min/max/p2/p98) + un jeton `raster_token`.
POST /api/raster/restyle  (raster_token, palette, vmin, vmax, classes) → re-rend
    le PNG (rampe continue ou classes discrètes) → même logique que le restyle
    des rasters GEE. Renvoie png + légende.

Le résultat est une couche image côté client (overlay MapLibre) ajoutée au menu
Couches ; le restyle ne fait que remplacer l'URL data du PNG.

Déps : rasterio numpy Pillow.
"""
import io
import os
import json
import time
import uuid
import glob
import shutil
import base64
import tempfile

from fastapi import APIRouter, UploadFile, File, Form, HTTPException

router = APIRouter(prefix="/api/raster", tags=["raster"])

_MAX_SIDE = 2500
_JOBS = os.path.join(tempfile.gettempdir(), "raster_jobs")
_TTL = 3600


def _cleanup():
    try:
        now = time.time()
        for d in glob.glob(os.path.join(_JOBS, "*")):
            if now - os.path.getmtime(d) > _TTL:
                shutil.rmtree(d, ignore_errors=True)
    except Exception:
        pass


def _ramp(anchors):
    import numpy as np
    xs = np.linspace(0, 1, len(anchors))
    t = np.linspace(0, 1, 256)
    a = np.asarray(anchors, dtype=float)
    out = np.zeros((256, 3), np.uint8)
    for c in range(3):
        out[:, c] = np.clip(np.interp(t, xs, a[:, c]), 0, 255).astype(np.uint8)
    return out


_CMAP = _ramp([[68, 1, 84], [59, 82, 139], [33, 145, 140], [94, 201, 98], [253, 231, 37]])


def _ramp_from_hex(hexlist):
    """Liste de couleurs hex → table 256×3 (rampe)."""
    anchors = []
    for h in hexlist:
        h = (h or "").strip().lstrip("#")
        if len(h) == 6:
            anchors.append([int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)])
    if len(anchors) < 2:
        return _CMAP
    return _ramp(anchors)


def _render(band, mask, cmap, vmin, vmax, classes):
    """Rend une bande en PNG base64 (nodata transparent).

    classes=0 → rampe continue ; classes≥2 → N classes discrètes (+ légende).
    Renvoie (png_b64, legend).
    """
    import numpy as np
    from PIL import Image
    h, w = band.shape
    span = (vmax - vmin) or 1.0
    legend = []
    if classes and classes >= 2:
        binned = np.clip(np.nan_to_num((band - vmin) / span * classes, nan=0.0).astype(np.int32), 0, classes - 1)
        cidx = ((binned + 0.5) / classes * 255).astype(np.uint8)
        rgb = cmap[cidx]
        bounds = np.linspace(vmin, vmax, classes + 1)
        for i in range(classes):
            col = cmap[int((i + 0.5) / classes * 255)]
            legend.append({"label": f"{bounds[i]:.1f} – {bounds[i + 1]:.1f}",
                           "color": "#%02x%02x%02x" % (int(col[0]), int(col[1]), int(col[2]))})
    else:
        norm = np.clip((band - vmin) / span, 0, 1)
        idx = np.nan_to_num(norm * 255).astype(np.uint8)
        rgb = cmap[idx]
    rgba = np.zeros((h, w, 4), np.uint8)
    rgba[..., :3] = rgb
    rgba[..., 3] = np.where(mask, 255, 0).astype(np.uint8)
    buf = io.BytesIO()
    Image.fromarray(rgba, "RGBA").save(buf, "PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii"), legend


@router.post("/import")
async def raster_import(file: UploadFile = File(...)):
    try:
        import numpy as np
        import rasterio
        from rasterio.warp import calculate_default_transform, reproject, Resampling
        from rasterio.transform import array_bounds
        from PIL import Image
    except ImportError as _e:
        raise HTTPException(503, f"Dépendance raster manquante côté serveur : « {getattr(_e, 'name', _e)} ». Installez : pip install rasterio Pillow.")

    _cleanup()
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tf:
            tmp_path = tf.name
            while True:
                buf = await file.read(1 << 20)
                if not buf:
                    break
                tf.write(buf)

        try:
            src = rasterio.open(tmp_path)
        except Exception as e:
            raise HTTPException(422, f"GeoTIFF illisible : {e}")

        with src:
            if src.crs is None:
                raise HTTPException(422, "GeoTIFF sans CRS : impossible de le géoréférencer.")
            dst_crs = "EPSG:4326"
            transform, W, H = calculate_default_transform(src.crs, dst_crs, src.width, src.height, *src.bounds)
            if max(W, H) > _MAX_SIDE:
                sc = _MAX_SIDE / float(max(W, H))
                transform, W, H = calculate_default_transform(
                    src.crs, dst_crs, src.width, src.height, *src.bounds,
                    dst_width=max(1, int(W * sc)), dst_height=max(1, int(H * sc)))

            nb = min(src.count, 3)
            src_nd = src.nodata
            dst = np.zeros((nb, H, W), dtype=np.float32)
            for b in range(nb):
                reproject(source=rasterio.band(src, b + 1), destination=dst[b],
                          src_transform=src.transform, src_crs=src.crs,
                          dst_transform=transform, dst_crs=dst_crs,
                          resampling=Resampling.bilinear, src_nodata=src_nd, dst_nodata=np.nan)
            w4, s4, e4, n4 = array_bounds(H, W, transform)
            coords = [[w4, n4], [e4, n4], [e4, s4], [w4, s4]]

            multiband = src.count >= 3
            if multiband:
                rgba = np.zeros((H, W, 4), np.uint8)
                for b in range(3):
                    fin = dst[b][np.isfinite(dst[b])]
                    lo, hi = (np.percentile(fin, 2), np.percentile(fin, 98)) if fin.size else (0.0, 1.0)
                    if hi <= lo:
                        hi = lo + 1.0
                    rgba[..., b] = np.nan_to_num(np.clip((dst[b] - lo) / (hi - lo), 0, 1) * 255).astype(np.uint8)
                rgba[..., 3] = np.where(np.isfinite(dst).all(axis=0), 255, 0).astype(np.uint8)
                buf = io.BytesIO(); Image.fromarray(rgba, "RGBA").save(buf, "PNG")
                png_b64 = base64.b64encode(buf.getvalue()).decode("ascii")
                return {
                    "name": getattr(file, "filename", "raster.tif"), "src_crs": str(src.crs),
                    "width": int(W), "height": int(H), "bands": int(src.count),
                    "bbox": [float(w4), float(s4), float(e4), float(n4)],
                    "image_coordinates": coords, "png_b64": png_b64, "raster_token": None,
                }

            # ── Mono-bande : cache + stats (restyle possible) ──
            band = dst[0]
            mask = np.isfinite(band)
            fin = band[mask]
            if fin.size == 0:
                raise HTTPException(422, "Raster vide après reprojection.")
            vmin, vmax = float(np.min(fin)), float(np.max(fin))
            p2, p98 = float(np.percentile(fin, 2)), float(np.percentile(fin, 98))

            token = uuid.uuid4().hex[:16]
            jdir = os.path.join(_JOBS, token); os.makedirs(jdir, exist_ok=True)
            np.save(os.path.join(jdir, "band.npy"), band.astype(np.float32))
            with open(os.path.join(jdir, "meta.json"), "w") as mf:
                json.dump({"coords": coords, "bbox": [w4, s4, e4, n4]}, mf)

            png_b64, _ = _render(band, mask, _CMAP, p2, p98, 0)
            return {
                "name": getattr(file, "filename", "raster.tif"), "src_crs": str(src.crs),
                "width": int(W), "height": int(H), "bands": 1,
                "bbox": [float(w4), float(s4), float(e4), float(n4)],
                "image_coordinates": coords, "png_b64": png_b64,
                "raster_token": token,
                "vmin": round(p2, 3), "vmax": round(p98, 3),
                "data_min": round(vmin, 3), "data_max": round(vmax, 3),
            }
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


@router.post("/restyle")
def raster_restyle(
    raster_token: str = Form(...),
    palette: str = Form(""),           # "hex1,hex2,…"
    vmin: float = Form(...),
    vmax: float = Form(...),
    classes: int = Form(0),            # 0 = rampe continue ; ≥2 = classes discrètes
):
    import numpy as np
    if not str(raster_token).isalnum():
        raise HTTPException(400, "Jeton invalide.")
    jdir = os.path.join(_JOBS, str(raster_token))
    bpath = os.path.join(jdir, "band.npy")
    if not os.path.isfile(bpath):
        raise HTTPException(410, "Raster expiré côté serveur — réimportez le GeoTIFF.")
    band = np.load(bpath)
    mask = np.isfinite(band)
    cmap = _ramp_from_hex(palette.split(",")) if palette.strip() else _CMAP
    png_b64, legend = _render(band, mask, cmap, float(vmin), float(vmax), int(classes))
    os.utime(jdir, None)   # rafraîchit le TTL
    return {"png_b64": png_b64, "legend": legend, "vmin": vmin, "vmax": vmax, "classes": classes}
