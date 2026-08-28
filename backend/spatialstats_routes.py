"""
spatialstats_routes.py — Statistiques spatiales sur une couche vecteur.

POST /api/spatialstats/run {geojson, field, k}
    → autocorrélation spatiale GLOBALE (indice de Moran I + test par permutations)
      et hotspots LOCAUX (Getis-Ord Gi*) par entité. Poids = k plus proches
      voisins sur les centroïdes (fonctionne pour points ET polygones).

Sans dépendance lourde (pas de PySAL) : numpy + shapely (centroïdes). Sortie =
la FeatureCollection enrichie (gi_z, gi_p, gi_class) + le résumé Moran.
"""
from typing import Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/spatialstats", tags=["spatialstats"])

_MAX_N = 3000


class SpatialStatsReq(BaseModel):
    geojson: dict
    field: str
    k: int = 8
    permutations: int = 999


def _centroid(geom):
    from shapely.geometry import shape
    try:
        c = shape(geom).centroid
        if c.is_empty:
            return None
        return (float(c.x), float(c.y))
    except Exception:
        return None


def _norm_cdf(z):
    import math
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def _class_from_z(z):
    import math
    if not math.isfinite(z):
        return "non significatif"
    a = abs(z)
    lvl = "99%" if a > 2.58 else "95%" if a > 1.96 else "90%" if a > 1.65 else None
    if lvl is None:
        return "non significatif"
    return (f"point chaud {lvl}" if z > 0 else f"point froid {lvl}")


@router.post("/run")
def spatialstats_run(req: SpatialStatsReq):
    import numpy as np

    feats = (req.geojson or {}).get("features") or []
    if not feats:
        raise HTTPException(422, "Couche vide.")
    if len(feats) > _MAX_N:
        raise HTTPException(422, f"Trop d'entités ({len(feats)}) — limite {_MAX_N}.")

    field = req.field
    xs, ys, vals, idx = [], [], [], []
    for i, ft in enumerate(feats):
        props = (ft or {}).get("properties") or {}
        if field not in props:
            continue
        try:
            v = float(props[field])
        except (TypeError, ValueError):
            continue
        if not np.isfinite(v):
            continue
        c = _centroid((ft or {}).get("geometry"))
        if c is None:
            continue
        xs.append(c[0]); ys.append(c[1]); vals.append(v); idx.append(i)

    n = len(vals)
    if n < 10:
        raise HTTPException(422, f"Trop peu d'entités valides ({n}) avec le champ « {field} » numérique et une géométrie.")

    x = np.asarray(vals, dtype=np.float64)
    px = np.asarray(xs, dtype=np.float64)
    py = np.asarray(ys, dtype=np.float64)
    # correction longitude (≈ isotrope autour de la latitude moyenne)
    px = px * np.cos(np.radians(float(np.mean(py))))

    k = max(1, min(int(req.k or 8), n - 1))

    # k plus proches voisins (par ligne, sans matrice n×n complète)
    neigh = np.zeros((n, k), dtype=np.int64)
    for i in range(n):
        d2 = (px - px[i]) ** 2 + (py - py[i]) ** 2
        d2[i] = np.inf
        neigh[i] = np.argpartition(d2, k)[:k]

    xbar = float(np.mean(x))
    z = x - xbar
    den = float(np.sum(z * z))
    if den == 0:
        raise HTTPException(422, "Le champ est constant : autocorrélation indéfinie.")

    # ── Moran's I global (poids voisinage binaire, normalisé par S0) ──
    def moran(zvec):
        s = 0.0
        for i in range(n):
            s += zvec[i] * np.sum(zvec[neigh[i]])
        S0 = n * k  # nb total de liens (k par entité)
        return (n / S0) * (s / float(np.sum(zvec * zvec)))

    I = moran(z)
    EI = -1.0 / (n - 1)
    # test par permutations (référence empirique)
    perms = max(0, min(int(req.permutations or 999), 4999))
    p_perm = None
    if perms:
        rng = np.random.default_rng(12345)
        cnt = 0
        for _ in range(perms):
            zp = z[rng.permutation(n)]
            if abs(moran(zp) - EI) >= abs(I - EI):
                cnt += 1
        p_perm = (cnt + 1) / (perms + 1)

    if p_perm is not None and p_perm > 0.05:
        verdict = "aléatoire (non significatif)"
    elif I > EI:
        verdict = "agrégé (valeurs semblables voisines)"
    elif I < EI:
        verdict = "dispersé (valeurs opposées voisines)"
    else:
        verdict = "aléatoire"

    # ── Getis-Ord Gi* local (voisinage + soi, poids binaires) ──
    S = float(np.sqrt(max(np.mean(x * x) - xbar * xbar, 0.0)))
    out_feats = []
    gi_counts = {}
    for pos, i in enumerate(idx):
        star = np.append(neigh[pos], pos)          # inclut l'entité (Gi*)
        w = star.size                               # somme des poids (binaire)
        lag = float(np.sum(x[star]))
        denom = S * np.sqrt(max((n * w - w * w) / (n - 1), 1e-12))
        gz = (lag - xbar * w) / denom if denom > 0 else float("nan")
        gp = 2.0 * (1.0 - _norm_cdf(abs(gz))) if np.isfinite(gz) else None
        cls = _class_from_z(gz)
        gi_counts[cls] = gi_counts.get(cls, 0) + 1
        nf = dict(feats[i])
        np_ = dict((feats[i] or {}).get("properties") or {})
        np_.update({"gi_z": round(float(gz), 3) if np.isfinite(gz) else None,
                    "gi_p": round(float(gp), 4) if gp is not None else None,
                    "gi_class": cls})
        nf["properties"] = np_
        out_feats.append(nf)

    return {
        "moran": {"I": round(float(I), 4), "expected": round(float(EI), 4),
                  "p": None if p_perm is None else round(float(p_perm), 4),
                  "verdict": verdict, "n": n, "k": k, "permutations": perms},
        "giClasses": gi_counts,
        "geojson": {"type": "FeatureCollection", "features": out_feats},
        "message": f"Moran I = {round(float(I), 4)} ({verdict}) · {n} entités, {k} voisins.",
    }
