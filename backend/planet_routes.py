"""
planet_routes.py — Proxy de textures planétaires pour le viewer « Système solaire ».

GET /api/planet/texture/{body}
    → renvoie la texture équirectangulaire du corps (Soleil, planètes, Lune,
      anneaux de Saturne, fond étoilé) en MÊME ORIGINE. Indispensable : WebGL
      (Three.js) refuse une texture cross-origin sans en-têtes CORS, or la source
      (Solar System Scope, CC-BY 4.0) n'en envoie pas. On la met aussi en cache.

Sécurité : `body` est une CLÉ d'un dictionnaire fixe → pas d'URL arbitraire (pas
de SSRF). Aucune dépendance lourde (urllib stdlib).
"""
import os
import tempfile
import urllib.request

from fastapi import APIRouter, HTTPException
from fastapi.responses import Response

router = APIRouter(prefix="/api/planet", tags=["planet"])

# Textures 2k Solar System Scope (CC-BY 4.0) — attribution affichée côté client.
_BASE = "https://www.solarsystemscope.com/textures/download/{name}.{ext}"
_TEXTURES = {
    "sun": "2k_sun", "mercury": "2k_mercury", "venus": "2k_venus_surface",
    "earth": "2k_earth_daymap", "moon": "2k_moon", "mars": "2k_mars",
    "jupiter": "2k_jupiter", "saturn": "2k_saturn",
    "saturn_ring": "2k_saturn_ring_alpha",
    "uranus": "2k_uranus", "neptune": "2k_neptune",
    "stars": "2k_stars_milky_way",
}
_CACHE = os.path.join(tempfile.gettempdir(), "planet_textures")


@router.get("/list")
def planet_list():
    return {"bodies": list(_TEXTURES.keys())}


@router.get("/texture/{body}")
def planet_texture(body: str):
    if body not in _TEXTURES:
        raise HTTPException(404, "Corps céleste inconnu.")
    ext = "png" if body.endswith("_ring") else "jpg"
    os.makedirs(_CACHE, exist_ok=True)
    fp = os.path.join(_CACHE, f"{body}.{ext}")

    if not os.path.isfile(fp) or os.path.getsize(fp) == 0:
        url = _BASE.format(name=_TEXTURES[body], ext=ext)
        req = urllib.request.Request(url, headers={"User-Agent": "OpenMapAgents/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
            if not data:
                raise ValueError("réponse vide")
            with open(fp, "wb") as f:
                f.write(data)
        except Exception as e:
            raise HTTPException(502, f"Texture « {body} » indisponible : {e}")

    with open(fp, "rb") as f:
        data = f.read()
    media = "image/png" if ext == "png" else "image/jpeg"
    return Response(content=data, media_type=media,
                    headers={"Cache-Control": "public, max-age=2592000"})
