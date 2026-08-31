"""
routing_routes.py — Routage & isochrones via le BACKEND (jeton dans l'env
backend), pour que les outils Itinéraire / Isochrone du front ne dépendent PAS
du build (VITE_MAPBOX_TOKEN souvent absent au build).

POST /api/route/directions {waypoints, profile, alternatives?}
    → itinéraire(s) A→B (ORS si ORS_API_KEY, sinon Mapbox) avec étapes.
POST /api/route/isochrone  {center, minutes, profile, intervals?}
    → polygones isochrones (GeoJSON).

Le géocodage (autocomplétion d'adresse) reste côté client (Nominatim, sans clé).
"""
import os
import json
import urllib.request
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/route", tags=["routing"])

_ORS_KEY = os.getenv("ORS_API_KEY", "")
_ORS_BASE = os.getenv("ORS_BASE_URL", "https://api.openrouteservice.org")
_MAPBOX = os.getenv("MAPBOX_ACCESS_TOKEN") or os.getenv("VITE_MAPBOX_TOKEN") or ""
_ORS_PROF = {"foot": "foot-walking", "bike": "cycling-regular", "car": "driving-car", "hike": "foot-hiking"}
_MB_PROF = {"foot": "walking", "bike": "cycling", "car": "driving", "hike": "walking"}
_UA = {"User-Agent": "OpenMapAgents/1.0"}


class DirReq(BaseModel):
    waypoints: List[List[float]]     # [[lon,lat], …]
    profile: str = "foot"
    alternatives: bool = False


class IsoReq(BaseModel):
    center: List[float]              # [lon,lat]
    minutes: float = 10
    profile: str = "foot"
    intervals: Optional[List[float]] = None


def _get(url, timeout=25):
    with urllib.request.urlopen(urllib.request.Request(url, headers=_UA), timeout=timeout) as r:
        return json.load(r)


def _post(url, body, headers, timeout=25):
    h = {**_UA, **headers}
    rq = urllib.request.Request(url, data=json.dumps(body).encode(), headers=h)
    with urllib.request.urlopen(rq, timeout=timeout) as r:
        return json.load(r)


# ─────────────────────────── directions ───────────────────────────────────
def _ors_dir(wps, profile, alternatives):
    prof = _ORS_PROF.get(profile, "foot-walking")
    body = {"coordinates": [[float(p[0]), float(p[1])] for p in wps],
            "instructions": True, "instructions_format": "text", "language": "fr"}
    if alternatives and len(wps) == 2:
        body["alternative_routes"] = {"target_count": 3, "weight_factor": 1.6, "share_factor": 0.6}
    d = _post(f"{_ORS_BASE}/v2/directions/{prof}/geojson", body,
              {"Authorization": _ORS_KEY, "Content-Type": "application/json"})
    out = []
    for f in d.get("features", []):
        coords = (f.get("geometry") or {}).get("coordinates") or []
        props = f.get("properties") or {}
        summ = props.get("summary") or {}
        steps = []
        for seg in props.get("segments", []):
            for st in seg.get("steps", []):
                steps.append({"instruction": st.get("instruction", ""), "distance_m": round(st.get("distance", 0)),
                              "duration_s": round(st.get("duration", 0)), "name": st.get("name", "")})
        if coords:
            out.append({"coordinates": coords, "distance": summ.get("distance", 0), "duration": summ.get("duration", 0), "steps": steps})
    return out


def _mb_dir(wps, profile, alternatives):
    prof = _MB_PROF.get(profile, "walking")
    coords = ";".join(f"{p[0]},{p[1]}" for p in wps)
    alt = "true" if (alternatives and len(wps) == 2) else "false"
    url = (f"https://api.mapbox.com/directions/v5/mapbox/{prof}/{coords}"
           f"?alternatives={alt}&geometries=geojson&overview=full&steps=true&language=fr&access_token={_MAPBOX}")
    d = _get(url)
    out = []
    for rt in d.get("routes", []):
        c = (rt.get("geometry") or {}).get("coordinates") or []
        steps = []
        for leg in rt.get("legs", []):
            for st in leg.get("steps", []):
                steps.append({"instruction": (st.get("maneuver") or {}).get("instruction", ""),
                              "distance_m": round(st.get("distance", 0)), "duration_s": round(st.get("duration", 0)),
                              "name": st.get("name", "")})
        if c:
            out.append({"coordinates": c, "distance": rt.get("distance", 0), "duration": rt.get("duration", 0), "steps": steps})
    return out


@router.post("/directions")
def directions(req: DirReq):
    if not req.waypoints or len(req.waypoints) < 2:
        raise HTTPException(422, "Au moins 2 points requis.")
    routes, errors = [], []
    if _ORS_KEY:
        try:
            routes = _ors_dir(req.waypoints, req.profile, req.alternatives)
        except Exception as e:
            errors.append(f"ORS: {e}")
    if not routes and _MAPBOX:
        try:
            routes = _mb_dir(req.waypoints, req.profile, req.alternatives)
        except Exception as e:
            errors.append(f"Mapbox: {e}")
    if not routes:
        raise HTTPException(502, ("Routage en échec — " + " ; ".join(errors)) if errors
                            else "Routage indisponible : aucun jeton (ORS_API_KEY / MAPBOX_ACCESS_TOKEN) dans l'env backend.")
    return {"routes": routes}


# ─────────────────────────── isochrones ───────────────────────────────────
def _ors_iso(center, ranges_sec, profile):
    prof = _ORS_PROF.get(profile, "foot-walking")
    body = {"locations": [[float(center[0]), float(center[1])]], "range": ranges_sec, "range_type": "time", "smoothing": 0.5}
    return _post(f"{_ORS_BASE}/v2/isochrones/{prof}", body, {"Authorization": _ORS_KEY, "Content-Type": "application/json"})


def _mb_iso(center, minutes_list, profile):
    prof = _MB_PROF.get(profile, "walking")
    contours = ",".join(str(int(m)) for m in minutes_list[:4])
    url = (f"https://api.mapbox.com/isochrone/v1/mapbox/{prof}/{center[0]},{center[1]}"
           f"?contours_minutes={contours}&polygons=true&access_token={_MAPBOX}")
    return _get(url)


@router.post("/isochrone")
def isochrone(req: IsoReq):
    if not req.center or len(req.center) != 2:
        raise HTTPException(422, "Centre invalide [lon, lat].")
    if req.intervals:
        mins = list(req.intervals)
    elif req.minutes >= 15:
        mins = [5, 10, req.minutes]
    elif req.minutes >= 10:
        mins = [5, req.minutes]
    else:
        mins = [req.minutes]
    mins = sorted({round(float(m), 2) for m in mins if m and m > 0})
    if not mins:
        raise HTTPException(422, "Intervalle(s) invalide(s).")

    gj, provider, errors = None, None, []
    if _ORS_KEY:
        try:
            gj = _ors_iso(req.center, [int(m * 60) for m in mins], req.profile); provider = "ors"
        except Exception as e:
            errors.append(f"ORS: {e}")
    if (not gj or not gj.get("features")) and _MAPBOX:
        try:
            gj = _mb_iso(req.center, mins, req.profile); provider = "mapbox"
        except Exception as e:
            errors.append(f"Mapbox: {e}")
    if not gj or not gj.get("features"):
        raise HTTPException(502, ("Isochrone en échec — " + " ; ".join(errors)) if errors
                            else "Isochrone indisponible : aucun jeton (ORS_API_KEY / MAPBOX_ACCESS_TOKEN) dans l'env backend.")
    return {"geojson": gj, "intervals_min": mins, "provider": provider}
