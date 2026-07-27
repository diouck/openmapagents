"""
maps/routes.py — CRUD cartes + profils (GeoJSON) + partage public
"""

import json
import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import desc

from auth.core import get_db, generate_slug
from auth.models import Map, User
from auth.routes import current_user

log = logging.getLogger("maps")

router = APIRouter(prefix="/api/maps", tags=["maps"])


# ============================================================
# 📦 SCHEMAS
# ============================================================

class MapCreate(BaseModel):
    title: str
    description: str = ""
    state_json: str
    thumbnail: str = ""
    is_public: bool = False


class MapUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    state_json: Optional[str] = None
    thumbnail: Optional[str] = None
    is_public: Optional[bool] = None


# ✅ PROFIL AVEC GEOJSON
class Profil(BaseModel):
    id: str
    title: str
    desc: str = ""

    geometry: dict  # 👈 GeoJSON

    bbox: list
    stats: dict
    source: str = ""
    crossData: Optional[dict] = None
    options: dict
    crossLayers: list
    savedAt: str


# ============================================================
# 🧠 HELPERS PROFILS
# ============================================================

def _get_profils(user: User):
    try:
        return json.loads(user.profils) if user.profils else []
    except Exception:
        return []


def _save_profils(user: User, profils, db: Session):
    user.profils = json.dumps(profils)
    db.commit()


def _validate_geojson(geometry: dict):
    if not isinstance(geometry, dict):
        return False
    if "type" not in geometry:
        return False
    return True


# ============================================================
# 🗺️ MAPS
# ============================================================

def _map_out(m: Map) -> dict:
    return {
        "id": str(m.id),
        "title": m.title,
        "description": m.description,
        "slug": m.slug,
        "thumbnail": m.thumbnail,
        "is_public": m.is_public,
        "view_count": m.view_count,
        "created_at": m.created_at.isoformat(),
        "updated_at": m.updated_at.isoformat(),
    }


def _map_full(m: Map) -> dict:
    d = _map_out(m)
    d["state_json"] = m.state_json
    return d


# ============================================================
# 📂 MES CARTES
# ============================================================

@router.get("")
def list_my_maps(
    page: int = Query(1, ge=1),
    limit: int = Query(12, ge=1, le=50),
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    offset = (page - 1) * limit

    total = db.query(Map).filter(Map.user_id == user.id).count()

    maps = (
        db.query(Map)
        .filter(Map.user_id == user.id)
        .order_by(desc(Map.updated_at))
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {
        "maps": [_map_out(m) for m in maps],
        "total": total,
        "page": page,
        "limit": limit,
    }


@router.post("", status_code=201)
def create_map(
    body: MapCreate,
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    try:
        json.loads(body.state_json)
    except Exception:
        raise HTTPException(400, "state_json invalide")

    m = Map(
        user_id=user.id,
        title=body.title,
        description=body.description,
        slug=generate_slug(body.title),
        state_json=body.state_json,
        thumbnail=body.thumbnail,
        is_public=body.is_public,
    )

    db.add(m)
    db.commit()
    db.refresh(m)

    log.info(f"Carte créée : {m.title} ({m.slug}) par {user.email}")

    return _map_full(m)


@router.get("/my/{map_id}")
def get_my_map(
    map_id: str,
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    m = db.query(Map).filter(Map.id == map_id, Map.user_id == user.id).first()

    if not m:
        raise HTTPException(404, "Carte introuvable")

    return _map_full(m)


@router.patch("/my/{map_id}")
def update_map(
    map_id: str,
    body: MapUpdate,
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    m = db.query(Map).filter(Map.id == map_id, Map.user_id == user.id).first()

    if not m:
        raise HTTPException(404, "Carte introuvable")

    if body.title is not None:
        m.title = body.title

    if body.description is not None:
        m.description = body.description

    if body.state_json is not None:
        try:
            json.loads(body.state_json)
        except Exception:
            raise HTTPException(400, "state_json invalide")
        m.state_json = body.state_json

    if body.thumbnail is not None:
        m.thumbnail = body.thumbnail

    if body.is_public is not None:
        m.is_public = body.is_public

    db.commit()
    db.refresh(m)

    return _map_full(m)


@router.delete("/my/{map_id}", status_code=204)
def delete_map(
    map_id: str,
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    m = db.query(Map).filter(Map.id == map_id, Map.user_id == user.id).first()

    if not m:
        raise HTTPException(404, "Carte introuvable")

    db.delete(m)
    db.commit()


# ============================================================
# 🌍 PARTAGE PUBLIC
# ============================================================

@router.get("/share/{slug}")
def get_shared_map(slug: str, db: Session = Depends(get_db)):
    m = db.query(Map).filter(Map.slug == slug, Map.is_public == True).first()

    if not m:
        raise HTTPException(404, "Carte introuvable ou non publique")

    m.view_count += 1
    db.commit()

    return _map_full(m)


# ============================================================
# 🖼️ GALERIE
# ============================================================

@router.get("/gallery")
def gallery(
    page: int = Query(1, ge=1),
    limit: int = Query(12),
    db: Session = Depends(get_db),
):
    offset = (page - 1) * limit

    total = db.query(Map).filter(Map.is_public == True).count()

    maps = (
        db.query(Map)
        .filter(Map.is_public == True)
        .order_by(desc(Map.view_count))
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {"maps": [_map_out(m) for m in maps], "total": total}


# ============================================================
# 📊 PROFILS (GeoJSON)
# ============================================================

@router.get("/profils")
def get_profils(
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    profils = _get_profils(user)

    profils = sorted(profils, key=lambda x: x.get("savedAt", ""), reverse=True)

    return {"profils": profils}


@router.post("/profils")
def add_profil(
    profil: Profil,
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    profils = _get_profils(user)

    if len(profils) >= 10:
        raise HTTPException(400, "Quota de profils atteint (10 max)")

    if not _validate_geojson(profil.geometry):
        raise HTTPException(400, "geometry invalide (GeoJSON requis)")

    profils.append(profil.dict())

    _save_profils(user, profils, db)

    return {"profils": profils}


@router.put("/profils")
def replace_profils(
    profils: List[Profil],
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    if len(profils) > 10:
        raise HTTPException(400, "Max 10 profils")

    profils_json = []

    for p in profils:
        if not _validate_geojson(p.geometry):
            raise HTTPException(400, "geometry invalide")
        profils_json.append(p.dict())

    _save_profils(user, profils_json, db)

    return {"profils": profils_json}


@router.delete("/profils/{profil_id}")
def delete_profil(
    profil_id: str,
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    profils = _get_profils(user)

    new_profils = [p for p in profils if p.get("id") != profil_id]

    if len(new_profils) == len(profils):
        raise HTTPException(404, "Profil introuvable")

    _save_profils(user, new_profils, db)

    return {"profils": new_profils}