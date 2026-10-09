"""
dynamical_routes.py — Module « Données météo ouvertes » (dynamical.org).

dynamical.org publie ~27 jeux de données météo/climat en **Zarr/Icechunk**
(cloud-optimized), lus via xarray. Structure commune :

    init_time (run du modèle) × lead_time (échéance) × latitude × longitude → variables
    (+ ensemble_member pour les modèles d'ensemble GEFS / IFS-ENS / AIFS-ENS)

Le navigateur ne peut pas lire ces stores (14 PB en amont) : TOUT passe ici.
Le serveur ouvre le Zarr, extrait une tranche 2D (dataset + run + échéance +
variable [+ membre]), la colorise et renvoie un **overlay image PNG**
(`png_b64` + `image_coordinates`), que le frontend ajoute en couche `kind:"image"`.

Accès (host `data.dynamical.org` retiré le 2026-09-30) : on passe par le paquet
`dynamical-catalog` (`dynamical_catalog.open(id, chunks=None)`), qui résout seul
l'asset Icechunk du STAC. Deps : dynamical-catalog>=0.8.0, icechunk, xarray, numpy.

Montée en charge : v1 validée sur `noaa-gfs-forecast`, généralisable aux autres
datasets (mêmes dimensions). Caches mémoire : catalogue, datasets ouverts, dims.
"""
import io
import base64
import threading
import logging

import numpy as np
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger("dynamical")
router = APIRouter(prefix="/dynamical", tags=["dynamical"])

# ─────────────────────────────────────────────────────────────────────────────
# Catalogue statique (sélecteur « Dataset »). Source : dynamical.org/catalog.
# `members` = dataset d'ensemble ; `lead_h` = échéance max en heures.
# ─────────────────────────────────────────────────────────────────────────────
CATALOG = [
    # --- NOAA GFS ---
    {"id": "noaa-gfs-forecast", "model": "NOAA GFS", "kind": "forecast", "vars": 25, "lead_h": 384, "res": "0.25°", "members": False, "domain": "Global"},
    {"id": "noaa-gfs-analysis", "model": "NOAA GFS", "kind": "analysis", "vars": 25, "lead_h": 0,   "res": "0.25°", "members": False, "domain": "Global"},
    # --- NOAA GEFS (ensemble) ---
    {"id": "noaa-gefs-forecast-35-day", "model": "NOAA GEFS", "kind": "forecast", "vars": 27, "lead_h": 840, "res": "0.25/0.5°", "members": True, "domain": "Global"},
    {"id": "noaa-gefs-analysis", "model": "NOAA GEFS", "kind": "analysis", "vars": 27, "lead_h": 0, "res": "0.25°", "members": True, "domain": "Global"},
    # --- NOAA HRRR (CONUS) ---
    {"id": "noaa-hrrr-forecast-48-hour", "model": "NOAA HRRR", "kind": "forecast", "vars": 27, "lead_h": 48, "res": "3 km", "members": False, "domain": "CONUS"},
    {"id": "noaa-hrrr-analysis", "model": "NOAA HRRR", "kind": "analysis", "vars": 27, "lead_h": 0, "res": "3 km", "members": False, "domain": "CONUS"},
    # --- ECMWF ---
    {"id": "ecmwf-aifs-single-forecast", "model": "ECMWF AIFS", "kind": "forecast", "vars": 17, "lead_h": 360, "res": "0.25°", "members": False, "domain": "Global"},
    {"id": "ecmwf-ifs-ens-forecast-15-day-0-25-degree", "model": "ECMWF IFS-ENS", "kind": "forecast", "vars": 19, "lead_h": 360, "res": "0.25°", "members": True, "domain": "Global"},
    {"id": "ecmwf-ifs-ens-forecast-46-day-daily-1-5-degree", "model": "ECMWF IFS-ENS", "kind": "forecast", "vars": 41, "lead_h": 1104, "res": "1.5°", "members": True, "domain": "Global"},
    # --- DWD ICON-EU ---
    {"id": "dwd-icon-eu-forecast-5-day", "model": "DWD ICON-EU", "kind": "forecast", "vars": 19, "lead_h": 120, "res": "0.0625°", "members": False, "domain": "Europe"},
    # --- NASA IMERG (précipitations) ---
    {"id": "nasa-imerg-analysis-late", "model": "NASA IMERG", "kind": "analysis", "vars": 2, "lead_h": 0, "res": "0.1°", "members": False, "domain": "Global"},
    # --- NOAA MRMS (CONUS) ---
    {"id": "noaa-mrms-conus-analysis-hourly", "model": "NOAA MRMS", "kind": "analysis", "vars": 6, "lead_h": 0, "res": "0.01°", "members": False, "domain": "CONUS"},
    # --- ECCC HRDPS (Canada) ---
    {"id": "eccc-hrdps-forecast", "model": "ECCC HRDPS", "kind": "forecast", "vars": 17, "lead_h": 48, "res": "2.5 km", "members": False, "domain": "Canada"},
]
_CAT_IDS = {c["id"] for c in CATALOG}

# ─────────────────────────────────────────────────────────────────────────────
# Rendu : palette + plage par variable (heuristique sur le nom).
# ─────────────────────────────────────────────────────────────────────────────
_PAL_TEMP   = ["#313695", "#4575b4", "#74add1", "#abd9e9", "#e0f3f8", "#ffffbf", "#fee090", "#fdae61", "#f46d43", "#d73027", "#a50026"]
_PAL_PRECIP = ["#ffffff", "#c7e9c0", "#74c476", "#31a354", "#2171b5", "#6a51a3", "#ae017e", "#fcc5c0"]
_PAL_WIND   = ["#ffffff", "#c6dbef", "#6baed6", "#2171b5", "#08306b", "#54278f", "#a50f15"]
_PAL_GREY   = ["#08306b", "#4292c6", "#c6dbef", "#ffffff"]
_PAL_RH     = ["#8c510a", "#d8b365", "#f6e8c3", "#c7eae5", "#5ab4ac", "#01665e"]


def _vis_for(var, units):
    """Plage + palette + conversion d'affichage selon la variable."""
    v = var.lower()
    if "temperature" in v:
        return {"min": -30, "max": 45, "palette": _PAL_TEMP, "label": "°C", "conv": None}
    if "precipitation_surface" in v:  # kg m-2 s-1 → mm/h
        return {"min": 0, "max": 20, "palette": _PAL_PRECIP, "label": "mm/h", "conv": lambda a: a * 3600.0}
    if "precipitable_water" in v or "precipitation" in v:
        return {"min": 0, "max": 60, "palette": _PAL_PRECIP, "label": units, "conv": None}
    if v.startswith("wind_") or "wind" in v:
        return {"min": -30, "max": 30, "palette": _PAL_WIND, "label": "m/s", "conv": None}
    if "relative_humidity" in v:
        return {"min": 0, "max": 100, "palette": _PAL_RH, "label": "%", "conv": None}
    if "cloud_cover" in v or "percent" in v:
        return {"min": 0, "max": 100, "palette": _PAL_GREY, "label": "%", "conv": None}
    if "pressure" in v:  # Pa → hPa
        return {"min": 980, "max": 1040, "palette": _PAL_TEMP, "label": "hPa", "conv": lambda a: a / 100.0}
    if "radiation" in v:
        return {"min": 0, "max": 1000, "palette": _PAL_TEMP, "label": "W/m²", "conv": None}
    return {"min": None, "max": None, "palette": _PAL_TEMP, "label": units or "", "conv": None}


def _hex(h):
    h = h.lstrip("#")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def _colormap(arr, mn, mx, palette):
    """arr float2D → RGBA uint8 (palette linéaire) ; NaN → transparent."""
    pal = np.array([_hex(c) for c in palette], dtype=float)
    a = np.clip((arr - mn) / max(1e-9, (mx - mn)), 0, 1)
    n = len(pal) - 1
    idx = a * n
    lo = np.clip(np.floor(idx).astype(int), 0, n - 1)
    hi = np.clip(lo + 1, 0, n)
    frac = (idx - lo)[..., None]
    rgb = pal[lo] * (1 - frac) + pal[hi] * frac
    rgba = np.dstack([rgb.astype(np.uint8), np.full(arr.shape, 255, np.uint8)])
    rgba[..., 3] = np.where(~np.isfinite(arr), 0, 255).astype(np.uint8)
    return rgba


def _png_b64(rgba):
    from PIL import Image
    img = Image.fromarray(rgba, "RGBA")
    buf = io.BytesIO()
    img.save(buf, "PNG")
    return base64.b64encode(buf.getvalue()).decode()


# ─────────────────────────────────────────────────────────────────────────────
# Ouverture paresseuse + caches.
# ─────────────────────────────────────────────────────────────────────────────
_DS = {}          # id → xarray.Dataset (ouvert une fois)
_DIMS = {}        # id → métadonnées de dimensions
_LOCK = threading.Lock()


def _open(ds_id):
    if ds_id not in _CAT_IDS:
        raise HTTPException(404, f"Dataset inconnu : {ds_id}")
    with _LOCK:
        if ds_id in _DS:
            return _DS[ds_id]
        try:
            import dynamical_catalog
        except ImportError as e:
            raise HTTPException(503, f"Dépendance manquante : {e}. Installez `dynamical-catalog` + `icechunk`.")
        try:
            ds = dynamical_catalog.open(ds_id, chunks=None)
        except Exception as e:
            raise HTTPException(502, f"Ouverture de {ds_id} impossible : {e}")
        _DS[ds_id] = ds
        return ds


def _lat_name(ds):
    for c in ("latitude", "lat"):
        if c in ds.coords or c in ds.dims:
            return c
    raise HTTPException(500, "Coordonnée latitude introuvable.")


def _lon_name(ds):
    for c in ("longitude", "lon"):
        if c in ds.coords or c in ds.dims:
            return c
    raise HTTPException(500, "Coordonnée longitude introuvable.")


def _member_name(ds):
    for c in ("ensemble_member", "member", "realization", "number"):
        if c in ds.dims:
            return c
    return None


def _lead_hours(ds):
    lt = ds["lead_time"].values
    # timedelta64 → heures ; sinon suppose des secondes.
    if np.issubdtype(lt.dtype, np.timedelta64):
        return (lt / np.timedelta64(1, "h")).astype(float)
    return (lt.astype(float) / 3600.0)


def _dims(ds_id):
    if ds_id in _DIMS:
        return _DIMS[ds_id]
    ds = _open(ds_id)
    latn, lonn = _lat_name(ds), _lon_name(ds)
    init = ds["init_time"].values if "init_time" in ds else None
    inits = []
    if init is not None:
        inits = [str(np.datetime_as_string(t, unit="h")) + "Z" for t in np.atleast_1d(init)[-24:]]
    leads = _lead_hours(ds)
    lat = ds[latn].values
    lon = ds[lonn].values
    variables = []
    for name, da in ds.data_vars.items():
        if latn in da.dims and lonn in da.dims:
            variables.append({"id": name, "units": str(da.attrs.get("units", ""))})
    memn = _member_name(ds)
    out = {
        "id": ds_id,
        "init_times": inits,
        "lead_hours": [round(float(h), 2) for h in leads.tolist()],
        "variables": sorted(variables, key=lambda v: v["id"]),
        "members": int(ds.sizes[memn]) if memn else 0,
        "bbox": [float(np.min(lon)), float(np.min(lat)), float(np.max(lon)), float(np.max(lat))],
    }
    _DIMS[ds_id] = out
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Endpoints.
# ─────────────────────────────────────────────────────────────────────────────
@router.get("/catalog")
def catalog():
    """Liste des datasets pour le sélecteur (groupés par modèle côté frontend)."""
    return {"datasets": CATALOG}


@router.get("/dataset/{ds_id}")
def dataset(ds_id: str):
    """Dimensions réelles d'un dataset : runs, échéances, variables, membres, bbox."""
    return _dims(ds_id)


class FieldReq(BaseModel):
    dataset: str
    variable: str
    init_time: str | None = None      # ex. "2025-01-01T00Z" ; défaut = dernier run
    lead_hours: float = 0
    member: int | None = None
    bbox: list[float] | None = None   # [W,S,E,N] en 4326 ; défaut = globe/domaine
    max_px: int = 1600                # borne la taille de l'image


class PointReq(BaseModel):
    dataset: str
    variable: str
    lat: float
    lon: float
    init_time: str | None = None
    member: int | None = None


def _select(ds, req_variable, init_time, lead_hours, member):
    if req_variable not in ds.data_vars:
        raise HTTPException(404, f"Variable inconnue : {req_variable}")
    da = ds[req_variable]
    # init_time : valeur demandée (plus proche) ou dernier run disponible.
    if "init_time" in da.dims:
        if init_time:
            it = np.datetime64(init_time.replace("Z", ""))
            da = da.sel(init_time=it, method="nearest")
        else:
            da = da.isel(init_time=-1)
    # lead_time : plus proche de l'échéance demandée (en heures).
    if "lead_time" in da.dims:
        leads = _lead_hours(ds)
        i = int(np.argmin(np.abs(leads - float(lead_hours))))
        da = da.isel(lead_time=i)
    # membre d'ensemble.
    memn = _member_name(ds)
    if memn and memn in da.dims:
        da = da.isel({memn: int(member or 0)})
    return da


@router.post("/field")
def field(req: FieldReq):
    """Extrait une tranche 2D → overlay image PNG colorisé + légende."""
    ds = _open(req.dataset)
    latn, lonn = _lat_name(ds), _lon_name(ds)
    da = _select(ds, req.variable, req.init_time, req.lead_hours, req.member)

    # Emprise : subset bbox (gère l'ordre décroissant des latitudes).
    lat = ds[latn].values
    if req.bbox:
        w, s, e, n = req.bbox
        lat_dec = lat[0] > lat[-1]
        da = da.sel({latn: slice(n, s)} if lat_dec else {latn: slice(s, n)})
        da = da.sel({lonn: slice(w, e)})

    # Sous-échantillonnage pour borner la taille.
    ny, nx = da.sizes[latn], da.sizes[lonn]
    if max(ny, nx) > req.max_px:
        step = int(np.ceil(max(ny, nx) / req.max_px))
        da = da.isel({latn: slice(None, None, step), lonn: slice(None, None, step)})

    arr = np.asarray(da.values, dtype=float)
    sub_lat = da[latn].values
    sub_lon = da[lonn].values
    if arr.ndim != 2 or arr.size == 0:
        raise HTTPException(422, "Tranche vide pour ces paramètres / cette emprise.")
    # Latitudes croissantes en indices image (nord en haut).
    if sub_lat[0] < sub_lat[-1]:
        arr = arr[::-1, :]
        sub_lat = sub_lat[::-1]

    vis = _vis_for(req.variable, str(ds[req.variable].attrs.get("units", "")))
    if vis["conv"]:
        arr = vis["conv"](arr)
    mn = vis["min"] if vis["min"] is not None else float(np.nanmin(arr))
    mx = vis["max"] if vis["max"] is not None else float(np.nanmax(arr))
    rgba = _colormap(arr, mn, mx, vis["palette"])
    png = _png_b64(rgba)

    W, E = float(np.min(sub_lon)), float(np.max(sub_lon))
    S, N = float(np.min(sub_lat)), float(np.max(sub_lat))
    # Coins pour une source "image" Mapbox/MapLibre : TL, TR, BR, BL.
    coords = [[W, N], [E, N], [E, S], [W, S]]
    legend = []
    for i, c in enumerate(vis["palette"]):
        val = round(mn + (mx - mn) * i / (len(vis["palette"]) - 1), 1)
        legend.append({"color": c, "value": val, "label": f"{val} {vis['label']}".strip()})
    return {
        "status": "ok", "dataset": req.dataset, "variable": req.variable,
        "png_b64": png, "image_coordinates": coords, "bbox": [W, S, E, N],
        "legend": legend, "unit": vis["label"], "vmin": mn, "vmax": mx,
    }


@router.post("/point")
def point(req: PointReq):
    """Série temporelle au point (valeur par échéance) → graphe côté frontend."""
    ds = _open(req.dataset)
    latn, lonn = _lat_name(ds), _lon_name(ds)
    if req.variable not in ds.data_vars:
        raise HTTPException(404, f"Variable inconnue : {req.variable}")
    da = ds[req.variable]
    if "init_time" in da.dims:
        if req.init_time:
            da = da.sel(init_time=np.datetime64(req.init_time.replace("Z", "")), method="nearest")
        else:
            da = da.isel(init_time=-1)
    memn = _member_name(ds)
    if memn and memn in da.dims:
        da = da.isel({memn: int(req.member or 0)})
    # Normalise la longitude au référentiel du dataset.
    lon0 = req.lon
    if float(ds[lonn].values.min()) >= 0 and lon0 < 0:
        lon0 += 360
    da = da.sel({latn: req.lat, lonn: lon0}, method="nearest")

    vis = _vis_for(req.variable, str(ds[req.variable].attrs.get("units", "")))
    vals = np.asarray(da.values, dtype=float)
    if vis["conv"]:
        vals = vis["conv"](vals)
    leads = _lead_hours(ds) if "lead_time" in da.dims else np.array([0.0])
    series = [{"lead_h": round(float(h), 1), "value": (None if not np.isfinite(v) else round(float(v), 2))}
              for h, v in zip(np.atleast_1d(leads), np.atleast_1d(vals))]
    return {"status": "ok", "dataset": req.dataset, "variable": req.variable,
            "unit": vis["label"], "lat": req.lat, "lon": req.lon, "series": series}
