"""
worldbank/wb_routes.py — Endpoints FastAPI pour les données World Bank
À inclure dans backend.py :

    from worldbank.wb_routes import router as wb_router
    app.include_router(wb_router)
"""

import logging
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

from .indicators import INDICATORS, INDICATORS_BY_THEME, find_indicator_by_keyword, ALL_CODES
from .fetcher import build_choropleth, fetch_latest_year

log = logging.getLogger("worldbank.routes")
router = APIRouter(prefix="/api/worldbank", tags=["worldbank"])


# ─── MODÈLES ─────────────────────────────────────────────────────────────────

class ChoroplethRequest(BaseModel):
    indicator:  str            # code WB ex: "SP.POP.TOTL"
    year:       Optional[int] = None   # None → dernière année dispo
    keyword:    Optional[str] = None   # alternative au code (ex: "population")


# ─── ENDPOINTS ───────────────────────────────────────────────────────────────

@router.get("/indicators")
def list_indicators():
    """Liste tous les indicateurs supportés, groupés par thème."""
    return {
        "indicators": INDICATORS,
        "by_theme": INDICATORS_BY_THEME,
        "total": len(INDICATORS),
    }


@router.post("/choropleth")
def get_choropleth(req: ChoroplethRequest):
    """
    Retourne un GeoJSON choroplèthe mondial pour un indicateur donné.
    Joint les données World Bank avec les polygones pays.

    Exemples:
      {"indicator": "SP.POP.TOTL"}               → population mondiale
      {"indicator": "NY.GDP.PCAP.CD", "year": 2022} → PIB/hab 2022
      {"keyword": "espérance de vie"}             → trouve l'indicateur automatiquement
    """
    code = req.indicator

    # Résolution par mot-clé si pas de code direct
    if req.keyword and (not code or code not in INDICATORS):
        code = find_indicator_by_keyword(req.keyword)
        log.info(f"Keyword '{req.keyword}' → code '{code}'")

    if not code or code not in INDICATORS:
        raise HTTPException(400, f"Indicateur inconnu: '{code}'. Utilisez GET /api/worldbank/indicators pour la liste.")

    meta = INDICATORS[code]

    # Résolution de l'année
    if req.year:
        year = req.year
        geojson = build_choropleth(code, year, meta["label_fr"], meta["unit"])
    else:
        year, _ = fetch_latest_year(code)
        geojson = build_choropleth(code, year, meta["label_fr"], meta["unit"])

    if not geojson["features"]:
        raise HTTPException(404, f"Aucune donnée disponible pour {code} ({year})")

    return geojson


@router.get("/search")
def search_indicator(q: str = Query(..., description="Mot-clé en fr ou en")):
    """Trouve l'indicateur le plus pertinent pour un mot-clé."""
    code = find_indicator_by_keyword(q)
    if not code:
        raise HTTPException(404, "Aucun indicateur trouvé")
    return {"code": code, **INDICATORS[code]}
