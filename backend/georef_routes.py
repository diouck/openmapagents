"""
georef_routes.py — Géoréférenceur : caler une image (plan scanné, photo aérienne)
sur la carte à partir de points d'appui (GCP), puis la reprojeter en overlay.

POST /api/georef/warp {image_b64, gcps:[{px,py,lng,lat}], transform_type}
    → ajuste une transformation pixel→monde (affine, moindres carrés, ≥3 GCP ;
      projective/homographie ≥4 GCP), warpe l'image dans une grille EPSG:3857 et
      renvoie un PNG géoréférencé + coins lon/lat + RMSE (qualité du calage).

L'overlay se branche sur addImageLayer, comme un GeoTIFF importé.
Déps : numpy scipy Pillow rasterio.
"""
import io
import base64
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/georef", tags=["georef"])

_MAX_SRC = 5000     # côté max de l'image source acceptée
_MAX_OUT = 2000     # côté max de la grille de sortie


class GCP(BaseModel):
    px: float        # colonne (pixel), origine coin haut-gauche
    py: float        # ligne (pixel)
    lng: float
    lat: float


class WarpReq(BaseModel):
    image_b64: str
    gcps: List[GCP]
    transform_type: Optional[str] = "affine"   # "affine" | "projective"
    name: Optional[str] = None


def _fit_affine(uv, XY):
    """Ajuste [X,Y] = M·[u,v,1] par moindres carrés. Renvoie M (2×3)."""
    import numpy as np
    A = np.column_stack([uv[:, 0], uv[:, 1], np.ones(len(uv))])
    mx, *_ = np.linalg.lstsq(A, XY[:, 0], rcond=None)
    my, *_ = np.linalg.lstsq(A, XY[:, 1], rcond=None)
    return np.array([mx, my])          # [[ax,bx,cx],[ay,by,cy]]


def _fit_projective(uv, XY):
    """Homographie pixel→monde (DLT, moindres carrés). Renvoie H (3×3)."""
    import numpy as np
    n = len(uv)
    A = []
    for i in range(n):
        u, v = uv[i]; X, Y = XY[i]
        A.append([u, v, 1, 0, 0, 0, -X * u, -X * v, -X])
        A.append([0, 0, 0, u, v, 1, -Y * u, -Y * v, -Y])
    A = np.asarray(A, dtype=np.float64)
    _, _, Vt = np.linalg.svd(A)
    H = Vt[-1].reshape(3, 3)
    if abs(H[2, 2]) > 1e-12:
        H = H / H[2, 2]
    return H


@router.post("/warp")
def georef_warp(req: WarpReq):
    try:
        import numpy as np
        from PIL import Image
        from scipy.ndimage import map_coordinates
        from rasterio.warp import transform as warp_transform
    except ImportError as e:
        raise HTTPException(503, f"Dépendance manquante : « {getattr(e, 'name', e)} » (numpy/scipy/Pillow/rasterio requis).")

    gcps = req.gcps or []
    ttype = (req.transform_type or "affine").lower()
    need = 4 if ttype == "projective" else 3
    if len(gcps) < need:
        raise HTTPException(422, f"{need} points d'appui minimum pour une transformation {ttype} (fournis : {len(gcps)}).")

    # image
    try:
        raw = base64.b64decode(req.image_b64.split(",")[-1])
        img = Image.open(io.BytesIO(raw)).convert("RGBA")
    except Exception as e:
        raise HTTPException(422, f"Image illisible : {e}")
    W, H = img.size
    if max(W, H) > _MAX_SRC:
        raise HTTPException(422, f"Image trop grande ({W}×{H}) — max {_MAX_SRC} px de côté.")
    src = np.asarray(img)                    # (H,W,4) uint8

    # GCP : pixel ↔ monde (EPSG:3857)
    uv = np.array([[g.px, g.py] for g in gcps], dtype=np.float64)
    lon = [g.lng for g in gcps]; lat = [g.lat for g in gcps]
    Xw, Yw = warp_transform("EPSG:4326", "EPSG:3857", lon, lat)
    XY = np.column_stack([Xw, Yw])

    # ajustement pixel→monde + fonction inverse monde→pixel
    if ttype == "projective":
        Hm = _fit_projective(uv, XY)
        try:
            Hinv = np.linalg.inv(Hm)
        except np.linalg.LinAlgError:
            raise HTTPException(422, "Points d'appui dégénérés (alignés ?) — homographie impossible.")

        def fwd(u, v):
            d = Hm[2, 0] * u + Hm[2, 1] * v + Hm[2, 2]
            return ((Hm[0, 0] * u + Hm[0, 1] * v + Hm[0, 2]) / d,
                    (Hm[1, 0] * u + Hm[1, 1] * v + Hm[1, 2]) / d)

        def inv(X, Y):
            d = Hinv[2, 0] * X + Hinv[2, 1] * Y + Hinv[2, 2]
            return ((Hinv[0, 0] * X + Hinv[0, 1] * Y + Hinv[0, 2]) / d,
                    (Hinv[1, 0] * X + Hinv[1, 1] * Y + Hinv[1, 2]) / d)
    else:
        M = _fit_affine(uv, XY)
        Alin = M[:, :2]                      # [[ax,bx],[ay,by]]
        c = M[:, 2]                          # [cx,cy]
        det = Alin[0, 0] * Alin[1, 1] - Alin[0, 1] * Alin[1, 0]
        if abs(det) < 1e-9:
            raise HTTPException(422, "Points d'appui dégénérés (alignés ?) — affine impossible.")
        Ainv = np.linalg.inv(Alin)

        def fwd(u, v):
            return (M[0, 0] * u + M[0, 1] * v + M[0, 2],
                    M[1, 0] * u + M[1, 1] * v + M[1, 2])

        def inv(X, Y):
            dx = X - c[0]; dy = Y - c[1]
            return (Ainv[0, 0] * dx + Ainv[0, 1] * dy,
                    Ainv[1, 0] * dx + Ainv[1, 1] * dy)

    # RMSE (mètres) sur les GCP
    pX, pY = fwd(uv[:, 0], uv[:, 1])
    rmse = float(np.sqrt(np.mean((pX - XY[:, 0]) ** 2 + (pY - XY[:, 1]) ** 2)))

    # emprise monde = coins de l'image transformés
    cu = np.array([0, W, W, 0], dtype=np.float64)
    cv = np.array([0, 0, H, H], dtype=np.float64)
    cX, cY = fwd(cu, cv)
    Xmin, Xmax = float(np.min(cX)), float(np.max(cX))
    Ymin, Ymax = float(np.min(cY)), float(np.max(cY))
    if not all(np.isfinite([Xmin, Xmax, Ymin, Ymax])) or Xmax <= Xmin or Ymax <= Ymin:
        raise HTTPException(422, "Emprise géographique invalide — vérifiez les points d'appui.")

    # grille de sortie (aspect du monde, borné)
    aspect = (Xmax - Xmin) / (Ymax - Ymin)
    if aspect >= 1:
        outW = min(_MAX_OUT, max(W, 256)); outH = max(1, int(round(outW / aspect)))
    else:
        outH = min(_MAX_OUT, max(H, 256)); outW = max(1, int(round(outH * aspect)))
    psx = (Xmax - Xmin) / outW
    psy = (Ymax - Ymin) / outH

    # pour chaque pixel de sortie → monde → pixel source → échantillonnage
    ox = Xmin + (np.arange(outW) + 0.5) * psx
    oy = Ymax - (np.arange(outH) + 0.5) * psy
    XX, YY = np.meshgrid(ox, oy)
    U, V = inv(XX, YY)

    valid = (U >= 0) & (U <= W - 1) & (V >= 0) & (V <= H - 1)
    coords = [V.ravel(), U.ravel()]
    out = np.zeros((outH, outW, 4), np.uint8)
    for ch in range(4):
        s = map_coordinates(src[:, :, ch], coords, order=1, mode="constant", cval=0).reshape(outH, outW)
        out[:, :, ch] = s
    alpha = out[:, :, 3].astype(np.float32) / 255.0
    out[:, :, 3] = np.where(valid & (alpha > 0), out[:, :, 3], 0).astype(np.uint8)

    # coins lon/lat (TL,TR,BR,BL) + bbox
    lon4, lat4 = warp_transform("EPSG:3857", "EPSG:4326",
                                [Xmin, Xmax, Xmax, Xmin], [Ymax, Ymax, Ymin, Ymin])
    image_coordinates = [[lon4[0], lat4[0]], [lon4[1], lat4[1]], [lon4[2], lat4[2]], [lon4[3], lat4[3]]]
    w4, e4 = min(lon4), max(lon4)
    s4, n4 = min(lat4), max(lat4)

    buf = io.BytesIO(); Image.fromarray(out, "RGBA").save(buf, "PNG")
    png_b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return {
        "name": req.name or "Image géoréférencée",
        "bands": 3,
        "bbox": [float(w4), float(s4), float(e4), float(n4)],
        "image_coordinates": image_coordinates,
        "png_b64": png_b64,
        "raster_token": None,
        "rmse_m": round(rmse, 2),
        "transform_type": ttype,
        "n_gcps": len(gcps),
    }
