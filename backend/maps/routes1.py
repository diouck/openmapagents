"""
maps/routes.py — CRUD cartes + partage public
"""
import json, logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import desc

from auth.core import get_db, generate_slug
from auth.models import Map
from auth.routes import current_user

log    = logging.getLogger("maps")
router = APIRouter(prefix="/api/maps", tags=["maps"])

# ── Schémas ───────────────────────────────────────────────────
class MapCreate(BaseModel):
    title:       str
    description: str        = ""
    state_json:  str        # JSON stringifié
    thumbnail:   str        = ""  # base64 PNG
    is_public:   bool       = False

class MapUpdate(BaseModel):
    title:       Optional[str]  = None
    description: Optional[str]  = None
    state_json:  Optional[str]  = None
    thumbnail:   Optional[str]  = None
    is_public:   Optional[bool] = None

def _map_out(m: Map) -> dict:
    return {
        "id":          str(m.id),
        "title":       m.title,
        "description": m.description,
        "slug":        m.slug,
        "thumbnail":   m.thumbnail,   # base64 complet pour affichage vignette
        "is_public":   m.is_public,
        "view_count":  m.view_count,
        "created_at":  m.created_at.isoformat(),
        "updated_at":  m.updated_at.isoformat(),
    }

def _map_full(m: Map) -> dict:
    d = _map_out(m)
    d["thumbnail"]  = m.thumbnail   # complet pour l'affichage
    d["state_json"] = m.state_json
    return d

# ── Mes cartes ────────────────────────────────────────────────
@router.get("")
def list_my_maps(
    page: int = Query(1, ge=1),
    limit: int = Query(12, ge=1, le=50),
    user=Depends(current_user),
    db: Session = Depends(get_db),
):
    offset = (page - 1) * limit
    total  = db.query(Map).filter(Map.user_id == user.id).count()
    maps   = (db.query(Map).filter(Map.user_id == user.id)
              .order_by(desc(Map.updated_at)).offset(offset).limit(limit).all())
    return {"maps": [_map_out(m) for m in maps], "total": total, "page": page, "limit": limit}

@router.post("", status_code=201)
def create_map(body: MapCreate, user=Depends(current_user), db: Session = Depends(get_db)):
    # Valider le JSON
    try: json.loads(body.state_json)
    except: raise HTTPException(400, "state_json invalide")
    m = Map(
        user_id=user.id, title=body.title, description=body.description,
        slug=generate_slug(body.title), state_json=body.state_json,
        thumbnail=body.thumbnail, is_public=body.is_public,
    )
    db.add(m); db.commit(); db.refresh(m)
    log.info(f"Carte créée : {m.title} ({m.slug}) par {user.email}")
    return _map_full(m)

@router.get("/my/{map_id}")
def get_my_map(map_id: str, user=Depends(current_user), db: Session = Depends(get_db)):
    m = db.query(Map).filter(Map.id == map_id, Map.user_id == user.id).first()
    if not m: raise HTTPException(404, "Carte introuvable")
    return _map_full(m)

@router.patch("/my/{map_id}")
def update_map(map_id: str, body: MapUpdate, user=Depends(current_user), db: Session = Depends(get_db)):
    m = db.query(Map).filter(Map.id == map_id, Map.user_id == user.id).first()
    if not m: raise HTTPException(404, "Carte introuvable")
    if body.title       is not None: m.title       = body.title
    if body.description is not None: m.description = body.description
    if body.state_json  is not None:
        try: json.loads(body.state_json)
        except: raise HTTPException(400, "state_json invalide")
        m.state_json = body.state_json
    if body.thumbnail   is not None: m.thumbnail   = body.thumbnail
    if body.is_public   is not None: m.is_public   = body.is_public
    db.commit(); db.refresh(m)
    return _map_full(m)

@router.delete("/my/{map_id}", status_code=204)
def delete_map(map_id: str, user=Depends(current_user), db: Session = Depends(get_db)):
    m = db.query(Map).filter(Map.id == map_id, Map.user_id == user.id).first()
    if not m: raise HTTPException(404, "Carte introuvable")
    db.delete(m); db.commit()

# ── Vue publique ──────────────────────────────────────────────
@router.get("/share/{slug}")
def get_shared_map(slug: str, db: Session = Depends(get_db)):
    m = db.query(Map).filter(Map.slug == slug, Map.is_public == True).first()
    if not m: raise HTTPException(404, "Carte introuvable ou non publique")
    m.view_count += 1
    db.commit()
    return _map_full(m)

# ── Galerie publique ──────────────────────────────────────────
@router.get("/gallery")
def gallery(page: int = Query(1, ge=1), limit: int = Query(12), db: Session = Depends(get_db)):
    offset = (page - 1) * limit
    total  = db.query(Map).filter(Map.is_public == True).count()
    maps   = (db.query(Map).filter(Map.is_public == True)
              .order_by(desc(Map.view_count)).offset(offset).limit(limit).all())
    return {"maps": [_map_out(m) for m in maps], "total": total}
