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
import ast
import json
import time
import uuid
import glob
import shutil
import base64
import tempfile
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from pydantic import BaseModel

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


def _hex_rgb(h):
    h = (h or "").strip().lstrip("#")
    if len(h) == 6:
        return [int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)]
    return [128, 128, 128]


def _render(band, mask, cmap, vmin, vmax, classes, bounds=None, class_colors=None):
    """Rend une bande en PNG base64 (nodata transparent).

    - bounds (bornes explicites, longueur ncl+1) → classes non uniformes (digitize),
      avec couleurs par classe (class_colors) si fournies. Sinon :
    - classes=0 → rampe continue ; classes≥2 → N classes égales.
    Renvoie (png_b64, legend).
    """
    import numpy as np
    from PIL import Image
    h, w = band.shape
    span = (vmax - vmin) or 1.0
    legend = []
    if bounds is not None and len(bounds) >= 3:
        bnds = np.asarray(bounds, dtype=float)
        ncl = len(bnds) - 1
        safe = np.nan_to_num(band, nan=float(bnds[0]) - 1.0)
        idx = np.clip(np.digitize(safe, bnds[1:-1]), 0, ncl - 1)
        if class_colors and len(class_colors) == ncl:
            pal = np.array([_hex_rgb(c) for c in class_colors], np.uint8)
        else:
            pal = np.array([cmap[int((i + 0.5) / ncl * 255)] for i in range(ncl)], np.uint8)
        rgb = pal[idx]
        for i in range(ncl):
            c = pal[i]
            legend.append({"label": f"{bnds[i]:.2f} – {bnds[i + 1]:.2f}",
                           "min": round(float(bnds[i]), 4), "max": round(float(bnds[i + 1]), 4),
                           "color": "#%02x%02x%02x" % (int(c[0]), int(c[1]), int(c[2]))})
    elif classes and classes >= 2:
        binned = np.clip(np.nan_to_num((band - vmin) / span * classes, nan=0.0).astype(np.int32), 0, classes - 1)
        cidx = ((binned + 0.5) / classes * 255).astype(np.uint8)
        rgb = cmap[cidx]
        bnds = np.linspace(vmin, vmax, classes + 1)
        for i in range(classes):
            col = cmap[int((i + 0.5) / classes * 255)]
            legend.append({"label": f"{bnds[i]:.1f} – {bnds[i + 1]:.1f}",
                           "min": round(float(bnds[i]), 4), "max": round(float(bnds[i + 1]), 4),
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
        from rasterio.warp import calculate_default_transform, reproject, Resampling, transform as warp_transform
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
            # Web Mercator (3857) et NON 4326 : l'overlay image MapLibre étire le PNG
            # LINÉAIREMENT dans la projection d'affichage (Mercator). Un PNG en 4326
            # (grille équirectangulaire) se décale/écrase verticalement. En 3857 la
            # grille du PNG coïncide avec l'affichage → alignement exact avec le fond.
            dst_crs = "EPSG:3857"
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
            # Bornes du raster reprojeté (mètres Mercator), puis coins convertis en
            # lon/lat : MapLibre attend les 4 coins de l'overlay en géographique.
            xmin, ymin, xmax, ymax = array_bounds(H, W, transform)
            _lons, _lats = warp_transform(dst_crs, "EPSG:4326",
                                          [xmin, xmax, xmax, xmin],
                                          [ymax, ymax, ymin, ymin])
            coords = [[_lons[0], _lats[0]], [_lons[1], _lats[1]],
                      [_lons[2], _lats[2]], [_lons[3], _lats[3]]]   # TL,TR,BR,BL
            w4, e4 = min(_lons), max(_lons)
            s4, n4 = min(_lats), max(_lats)

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
            # transform/crs de la grille reprojetée (EPSG:3857) : nécessaires aux
            # statistiques zonales (rasterisation des polygones sur la même grille).
            with open(os.path.join(jdir, "meta.json"), "w") as mf:
                json.dump({"coords": coords, "bbox": [w4, s4, e4, n4],
                           "transform": [float(v) for v in list(transform)[:6]],
                           "crs": dst_crs, "width": int(W), "height": int(H)}, mf)

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


def _class_bounds(fin, method, n, breaks_str, vmin, vmax):
    """Bornes des classes (longueur n+1) selon la méthode."""
    import numpy as np
    if method == "manual" and (breaks_str or "").strip():
        try:
            edges = sorted(set(float(x) for x in breaks_str.split(",") if x.strip() != ""))
        except Exception:
            edges = []
        if len(edges) >= 3:
            return edges
    if fin.size == 0:
        return list(np.linspace(vmin, vmax, n + 1))
    if method == "quantile":
        b = sorted(set(round(float(x), 6) for x in np.quantile(fin, np.linspace(0, 1, n + 1))))
        return b if len(b) >= 3 else list(np.linspace(vmin, vmax, n + 1))
    if method == "jenks":
        try:
            from gee_routes import _jenks
            rng = np.random.default_rng(0)
            v = fin if fin.size <= 3000 else rng.choice(fin, 3000, replace=False)
            internal = _jenks([float(x) for x in v], n)
            b = sorted(set([float(vmin)] + [float(x) for x in internal] + [float(vmax)]))
            return b if len(b) >= 3 else sorted(set(round(float(x), 6) for x in np.quantile(fin, np.linspace(0, 1, n + 1))))
        except Exception:
            return sorted(set(round(float(x), 6) for x in np.quantile(fin, np.linspace(0, 1, n + 1))))
    return list(np.linspace(vmin, vmax, n + 1))   # equal


@router.post("/restyle")
def raster_restyle(
    raster_token: str = Form(...),
    palette: str = Form(""),           # "hex1,hex2,…" (rampe)
    vmin: float = Form(...),
    vmax: float = Form(...),
    classes: int = Form(0),            # 0 = rampe continue ; ≥2 = classes
    classify: str = Form("equal"),     # equal | quantile | jenks | manual
    breaks: str = Form(""),            # bornes complètes (mode manuel) "v0,v1,…,vn"
    class_colors: str = Form(""),      # "hex1,…,hexN" (une couleur par classe)
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
    cc = [c for c in class_colors.split(",") if c.strip()] if class_colors.strip() else None

    bounds = None
    n = int(classes)
    if n >= 2 or (classify == "manual" and breaks.strip()):
        if classify == "manual" and breaks.strip():
            n = max(2, len([b for b in breaks.split(",") if b.strip() != ""]) - 1)
        bounds = _class_bounds(band[mask], classify, max(2, n), breaks, float(vmin), float(vmax))

    png_b64, legend = _render(band, mask, cmap, float(vmin), float(vmax), int(classes), bounds=bounds, class_colors=cc)
    os.utime(jdir, None)
    return {"png_b64": png_b64, "legend": legend, "vmin": vmin, "vmax": vmax,
            "classes": (len(bounds) - 1) if bounds else int(classes), "classify": classify}


# ─────────────────────────────────────────────────────────────────────────────
#  Analyse raster : statistiques zonales + calculatrice (map algebra)
#  Opèrent sur les rasters MONO-BANDE importés (jeton `raster_token` → band.npy
#  en cache, grille EPSG:3857 + transform/crs dans meta.json).
# ─────────────────────────────────────────────────────────────────────────────
def _job_dir(token):
    if not str(token or "").isalnum():
        raise HTTPException(400, "Jeton invalide.")
    return os.path.join(_JOBS, str(token))


def _load_job(token):
    """Charge (band, meta, jdir) d'un raster importé ; 410 si expiré/absent."""
    import numpy as np
    jdir = _job_dir(token)
    bpath = os.path.join(jdir, "band.npy")
    mpath = os.path.join(jdir, "meta.json")
    if not os.path.isfile(bpath) or not os.path.isfile(mpath):
        raise HTTPException(410, "Raster expiré côté serveur — réimportez le GeoTIFF.")
    band = np.load(bpath)
    with open(mpath) as f:
        meta = json.load(f)
    os.utime(jdir, None)   # rafraîchit le TTL
    return band, meta, jdir


class ZonalReq(BaseModel):
    raster_token: str
    zones: dict                       # GeoJSON FeatureCollection (polygones, WGS84)
    label_field: Optional[str] = None


@router.post("/zonal")
def raster_zonal(req: ZonalReq):
    """Statistiques zonales : agrège les pixels du raster par polygone.

    Rasterise chaque zone (reprojetée dans la grille du raster) en un label
    1..N, puis calcule count/min/moyenne/max/écart-type/somme par label en une
    passe (bincount). Renvoie un tableau + la FeatureCollection enrichie (zs_*).
    """
    import numpy as np
    try:
        from rasterio.transform import Affine
        from rasterio.features import rasterize
        from rasterio.warp import transform_geom
    except ImportError as e:
        raise HTTPException(503, f"Dépendance raster manquante : « {getattr(e, 'name', e)} ».")

    band, meta, _ = _load_job(req.raster_token)
    tr, crs = meta.get("transform"), meta.get("crs")
    if not tr or not crs:
        raise HTTPException(422, "Ce raster a été importé avant l'ajout des stats zonales — réimportez-le.")
    H, W = band.shape
    affine = Affine(*tr)

    feats = (req.zones or {}).get("features") or []
    if not feats:
        raise HTTPException(422, "Aucune zone (polygone) fournie.")
    if len(feats) > 2000:
        raise HTTPException(422, f"Trop de zones ({len(feats)}) — limite 2000.")

    shapes, labels_txt = [], []
    for i, ft in enumerate(feats):
        g = (ft or {}).get("geometry")
        props = (ft or {}).get("properties") or {}
        lbl = None
        if req.label_field and req.label_field in props:
            lbl = props.get(req.label_field)
        else:
            for k in ("name", "nom", "NAME", "NOM", "label", "id", "ID"):
                if k in props:
                    lbl = props[k]; break
        labels_txt.append(str(lbl) if lbl is not None else f"Zone {i + 1}")
        if not g:
            continue
        try:
            gp = transform_geom("EPSG:4326", crs, g)
            shapes.append((gp, i + 1))
        except Exception:
            pass
    if not shapes:
        raise HTTPException(422, "Aucune géométrie exploitable dans les zones.")

    lab = rasterize(shapes, out_shape=(H, W), transform=affine, fill=0, dtype="uint32")
    sel = np.isfinite(band) & (lab > 0)
    fl = lab[sel]
    fv = band[sel].astype(np.float64)
    N = len(feats)
    count = np.bincount(fl, minlength=N + 1).astype(np.int64)
    ssum = np.bincount(fl, weights=fv, minlength=N + 1)
    ssq = np.bincount(fl, weights=fv * fv, minlength=N + 1)
    gmin = np.full(N + 1, np.inf); np.minimum.at(gmin, fl, fv)
    gmax = np.full(N + 1, -np.inf); np.maximum.at(gmax, fl, fv)

    def _r(v):
        return None if v is None else round(float(v), 4)

    columns = ["zone", "count", "min", "mean", "max", "std", "sum"]
    rows, out_feats = [], []
    for i, ft in enumerate(feats):
        li, c = i + 1, int(count[i + 1])
        if c > 0:
            mean = ssum[li] / c
            std = max(ssq[li] / c - mean * mean, 0.0) ** 0.5
            mn, mx, sm = _r(gmin[li]), _r(gmax[li]), _r(ssum[li])
            mean, std = _r(mean), _r(std)
        else:
            mean = std = mn = mx = sm = None
        rec = {"zone": labels_txt[i], "count": c, "min": mn, "mean": mean, "max": mx, "std": std, "sum": sm}
        rows.append([rec[k] for k in columns])
        nf = dict(ft or {})
        np_ = dict((ft or {}).get("properties") or {})
        np_.update({"zs_count": c, "zs_min": mn, "zs_mean": mean, "zs_max": mx, "zs_std": std, "zs_sum": sm})
        nf["properties"] = np_
        out_feats.append(nf)

    covered = int((count[1:] > 0).sum())
    return {
        "columns": columns, "rows": rows,
        "zones": {"type": "FeatureCollection", "features": out_feats},
        "nZones": N, "covered": covered,
        "message": f"{covered}/{N} zone(s) couverte(s) par le raster.",
    }


# ── Calculatrice raster : évaluation sûre d'une expression (map algebra) ──────
def _calc_env(np):
    return {
        "where": np.where, "log": np.log, "log10": np.log10, "log2": np.log2,
        "sqrt": np.sqrt, "exp": np.exp, "abs": np.abs, "absolute": np.abs,
        "clip": np.clip, "minimum": np.minimum, "maximum": np.maximum,
        "sin": np.sin, "cos": np.cos, "tan": np.tan, "arctan": np.arctan,
        "floor": np.floor, "ceil": np.ceil, "power": np.power,
        "nan_to_num": np.nan_to_num, "isnan": np.isnan, "isfinite": np.isfinite,
        "pi": float(np.pi), "e": float(np.e), "nan": float("nan"),
    }


_ALLOWED_NODES = (
    ast.Expression, ast.BinOp, ast.UnaryOp, ast.Compare, ast.Call, ast.IfExp,
    ast.Name, ast.Load, ast.Constant, ast.Num,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow, ast.Mod, ast.FloorDiv,
    ast.BitAnd, ast.BitOr, ast.BitXor, ast.Invert, ast.USub, ast.UAdd,
    ast.Lt, ast.Gt, ast.LtE, ast.GtE, ast.Eq, ast.NotEq,
)


def _safe_calc(expr, band, np):
    """Évalue `expr` (variable A = le raster) sans eval libre : AST verrouillé."""
    if len(expr) > 500:
        raise HTTPException(422, "Expression trop longue (500 caractères max).")
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise HTTPException(422, f"Expression invalide : {e.msg}.")
    env = _calc_env(np)
    allowed = set(env.keys()) | {"A", "x"}
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise HTTPException(422, f"Élément non autorisé : {type(node).__name__}. Utilisez A, des nombres et les fonctions listées.")
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in env or node.keywords:
                raise HTTPException(422, "Appel de fonction non autorisé.")
        if isinstance(node, ast.Name) and node.id not in allowed:
            raise HTTPException(422, f"Nom inconnu : « {node.id} ». Le raster s'appelle A.")
        if isinstance(node, ast.Constant) and not isinstance(node.value, (int, float)):
            raise HTTPException(422, "Seules les constantes numériques sont autorisées.")
    scope = dict(env); scope["A"] = band; scope["x"] = band
    try:
        return eval(compile(tree, "<calc>", "eval"), {"__builtins__": {}}, scope)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(422, f"Erreur d'évaluation : {e}")


class CalcReq(BaseModel):
    raster_token: str
    expr: str
    name: Optional[str] = None
    palette: Optional[str] = ""


@router.post("/calc")
def raster_calc(req: CalcReq):
    """Calculatrice raster : applique une expression (A = le raster) et renvoie
    un NOUVEAU raster (même grille) affichable comme couche image."""
    import numpy as np
    band, meta, _ = _load_job(req.raster_token)
    expr = (req.expr or "").strip()
    if not expr:
        raise HTTPException(422, "Expression vide.")
    A = band.astype(np.float64)
    with np.errstate(all="ignore"):
        res = _safe_calc(expr, A, np)
    res = np.asarray(res)
    if res.dtype == bool:
        res = res.astype(np.float32)
    if res.shape != band.shape:
        if res.ndim == 0:
            res = np.full(band.shape, float(res), np.float32)
        else:
            raise HTTPException(422, "Le résultat n'a pas la forme du raster.")
    res = res.astype(np.float32)
    res[~np.isfinite(res)] = np.nan
    mask = np.isfinite(res)
    fin = res[mask]
    if fin.size == 0:
        raise HTTPException(422, "Résultat vide (aucune valeur définie).")
    vmin, vmax = float(np.min(fin)), float(np.max(fin))
    p2, p98 = float(np.percentile(fin, 2)), float(np.percentile(fin, 98))

    token = uuid.uuid4().hex[:16]
    jdir = os.path.join(_JOBS, token); os.makedirs(jdir, exist_ok=True)
    np.save(os.path.join(jdir, "band.npy"), res)
    with open(os.path.join(jdir, "meta.json"), "w") as mf:
        json.dump({"coords": meta.get("coords"), "bbox": meta.get("bbox"),
                   "transform": meta.get("transform"), "crs": meta.get("crs"),
                   "width": meta.get("width"), "height": meta.get("height")}, mf)

    cmap = _ramp_from_hex((req.palette or "").split(",")) if (req.palette or "").strip() else _CMAP
    png_b64, _ = _render(res, mask, cmap, p2, p98, 0)
    return {
        "name": req.name or f"calc · {expr[:40]}",
        "width": meta.get("width"), "height": meta.get("height"), "bands": 1,
        "bbox": meta.get("bbox"), "image_coordinates": meta.get("coords"),
        "png_b64": png_b64, "raster_token": token,
        "vmin": round(p2, 3), "vmax": round(p98, 3),
        "data_min": round(vmin, 3), "data_max": round(vmax, 3),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Vectorisation : polygones (polygonize) + courbes de niveau (contours)
#  Sortie GeoJSON en WGS84 (couche vecteur ajoutable à la carte).
# ─────────────────────────────────────────────────────────────────────────────
class PolygonizeReq(BaseModel):
    raster_token: str
    classes: int = 5
    simplify: float = 0.0        # tolérance de simplification (degrés), 0 = aucune


class ContoursReq(BaseModel):
    raster_token: str
    count: int = 10              # nombre de niveaux (si interval absent)
    interval: Optional[float] = None


_MAX_FEATURES = 20000
_MAX_VERTICES = 300000


@router.post("/polygonize")
def raster_polygonize(req: PolygonizeReq):
    """Découpe le raster en `classes` intervalles égaux et vectorise chaque
    classe en polygones (rasterio.features.shapes) → FeatureCollection WGS84."""
    import numpy as np
    try:
        from rasterio.transform import Affine
        from rasterio.features import shapes
        from rasterio.warp import transform_geom
    except ImportError as e:
        raise HTTPException(503, f"Dépendance raster manquante : « {getattr(e, 'name', e)} ».")

    band, meta, _ = _load_job(req.raster_token)
    tr, crs = meta.get("transform"), meta.get("crs")
    if not tr or not crs:
        raise HTTPException(422, "Ce raster a été importé avant l'ajout de la vectorisation — réimportez-le.")
    affine = Affine(*tr)
    n = max(2, min(int(req.classes or 5), 30))
    finite = np.isfinite(band)
    fin = band[finite]
    if fin.size == 0:
        raise HTTPException(422, "Raster vide.")
    vmin, vmax = float(np.min(fin)), float(np.max(fin))
    span = (vmax - vmin) or 1.0
    safe = np.where(finite, band, vmin)                       # évite NaN dans le cast int
    binned = np.clip(np.floor((safe - vmin) / span * n).astype(np.int32), 0, n - 1)
    binned = np.where(finite, binned, -1).astype(np.int32)   # -1 = nodata (masqué)

    feats = []
    try:
        gen = shapes(binned, mask=finite, transform=affine, connectivity=4)
        for geom, val in gen:
            v = int(val)
            if v < 0:
                continue
            g4326 = transform_geom(crs, "EPSG:4326", geom)
            lo = vmin + v * span / n
            hi = vmin + (v + 1) * span / n
            feats.append({"type": "Feature", "geometry": g4326,
                          "properties": {"class": v, "min": round(lo, 4), "max": round(hi, 4)}})
            if len(feats) > _MAX_FEATURES:
                break
    except Exception as e:
        raise HTTPException(500, f"Vectorisation impossible : {e}")

    truncated = len(feats) > _MAX_FEATURES
    return {
        "geojson": {"type": "FeatureCollection", "features": feats[:_MAX_FEATURES]},
        "count": min(len(feats), _MAX_FEATURES), "classes": n, "truncated": truncated,
        "message": f"{min(len(feats), _MAX_FEATURES)} polygone(s), {n} classes"
                   + (" (tronqué — réduisez le nombre de classes)" if truncated else "."),
    }


@router.post("/contours")
def raster_contours(req: ContoursReq):
    """Courbes de niveau (skimage.measure.find_contours) → LineStrings WGS84."""
    import numpy as np
    try:
        from rasterio.transform import Affine
        from rasterio.warp import transform as warp_transform
        from skimage import measure
    except ImportError as e:
        raise HTTPException(503, f"Dépendance manquante : « {getattr(e, 'name', e)} » (scikit-image requis pour les contours).")

    band, meta, _ = _load_job(req.raster_token)
    tr, crs = meta.get("transform"), meta.get("crs")
    if not tr or not crs:
        raise HTTPException(422, "Ce raster a été importé avant l'ajout des contours — réimportez-le.")
    affine = Affine(*tr)
    finite = np.isfinite(band)
    fin = band[finite]
    if fin.size == 0:
        raise HTTPException(422, "Raster vide.")
    vmin, vmax = float(np.min(fin)), float(np.max(fin))

    if req.interval and req.interval > 0:
        start = np.ceil(vmin / req.interval) * req.interval
        levels = list(np.arange(start, vmax, req.interval))
    else:
        c = max(2, min(int(req.count or 10), 60))
        levels = list(np.linspace(vmin, vmax, c + 2))[1:-1]
    if not levels:
        raise HTTPException(422, "Aucun niveau à tracer (ajustez l'intervalle).")
    if len(levels) > 200:
        raise HTTPException(422, f"Trop de niveaux ({len(levels)}) — augmentez l'intervalle.")

    # find_contours n'aime pas les NaN → on remplit sous le minimum (les
    # contours des niveaux réels ne traversent pas la zone nodata).
    filled = np.where(finite, band, vmin - 1.0).astype(np.float64)

    feats = []
    nverts = 0
    for lv in levels:
        try:
            contours = measure.find_contours(filled, float(lv))
        except Exception:
            continue
        for cont in contours:
            if len(cont) < 2:
                continue
            rows = cont[:, 0]; cols = cont[:, 1]
            # (row,col) index → monde (centre de pixel) via l'affine
            xs = affine.c + affine.a * (cols + 0.5) + affine.b * (rows + 0.5)
            ys = affine.f + affine.d * (cols + 0.5) + affine.e * (rows + 0.5)
            lon, lat = warp_transform(crs, "EPSG:4326", list(xs), list(ys))
            coords = [[round(lon[i], 6), round(lat[i], 6)] for i in range(len(lon))]
            feats.append({"type": "Feature",
                          "geometry": {"type": "LineString", "coordinates": coords},
                          "properties": {"level": round(float(lv), 4)}})
            nverts += len(coords)
            if nverts > _MAX_VERTICES:
                break
        if nverts > _MAX_VERTICES:
            break

    truncated = nverts > _MAX_VERTICES
    return {
        "geojson": {"type": "FeatureCollection", "features": feats},
        "count": len(feats), "levels": [round(float(x), 4) for x in levels], "truncated": truncated,
        "message": f"{len(feats)} isoligne(s) sur {len(levels)} niveau(x)"
                   + (" (tronqué)" if truncated else "."),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Export : couche image (overlay georéférencé, grille EPSG:3857 axis-aligned)
#  → GeoTIFF téléchargeable. Marche pour tout overlay (géoréférenceur, scène STAC,
#  raster importé…) : image PNG + 4 coins lon/lat.
# ─────────────────────────────────────────────────────────────────────────────
class ToGeotiffReq(BaseModel):
    image_b64: str                 # data URL ou base64 PNG
    coordinates: list              # [[lonTL,latTL],[lonTR,latTR],[lonBR,latBR],[lonBL,latBL]]
    name: Optional[str] = None


@router.post("/to_geotiff")
def raster_to_geotiff(req: ToGeotiffReq):
    import numpy as np
    from PIL import Image
    try:
        from rasterio.io import MemoryFile
        from rasterio.transform import from_bounds
        from rasterio.enums import ColorInterp
        from rasterio.warp import transform as warp_transform
    except ImportError as e:
        raise HTTPException(503, f"Dépendance raster manquante : « {getattr(e, 'name', e)} ».")

    try:
        raw = base64.b64decode((req.image_b64 or "").split(",")[-1])
        img = Image.open(io.BytesIO(raw)).convert("RGBA")
    except Exception as e:
        raise HTTPException(422, f"Image illisible : {e}")
    arr = np.asarray(img)
    H, W = arr.shape[:2]

    coords = req.coordinates or []
    if len(coords) < 4:
        raise HTTPException(422, "Emprise (4 coins lon/lat) manquante.")
    lons = [float(c[0]) for c in coords]; lats = [float(c[1]) for c in coords]
    xs, ys = warp_transform("EPSG:4326", "EPSG:3857", lons, lats)
    xmin, xmax = min(xs), max(xs); ymin, ymax = min(ys), max(ys)
    if xmax <= xmin or ymax <= ymin:
        raise HTTPException(422, "Emprise géographique invalide.")

    gt = from_bounds(xmin, ymin, xmax, ymax, W, H)
    try:
        with MemoryFile() as mf:
            with mf.open(driver="GTiff", height=H, width=W, count=4, dtype="uint8",
                         crs="EPSG:3857", transform=gt, compress="deflate",
                         photometric="RGB", tiled=True) as ds:
                for ch in range(4):
                    ds.write(arr[:, :, ch], ch + 1)
                ds.colorinterp = [ColorInterp.red, ColorInterp.green, ColorInterp.blue, ColorInterp.alpha]
            geotiff_b64 = base64.b64encode(mf.read()).decode("ascii")
    except Exception as e:
        raise HTTPException(500, f"Écriture GeoTIFF impossible : {e}")

    return {"geotiff_b64": geotiff_b64, "name": req.name or "couche", "width": W, "height": H}
