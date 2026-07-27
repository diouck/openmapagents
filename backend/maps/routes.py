"""
maps/routes.py  — avec validation UUID pour eviter DataError psycopg2
"""
import re, uuid, json, logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc
from sqlalchemy.orm import Session
from pydantic import BaseModel

from auth.models import Map, User
from auth.core import get_db, get_current_user

log = logging.getLogger("maps")


def _extract_bbox_wkt(state: dict) -> str | None:
    """Extrait le bbox WKT de toutes les couches du state_json.
    Retourne un POLYGON WKT ou None si pas de géométrie."""
    try:
        layers = state.get("layers", [])
        all_coords = []
        for l in layers:
            gj = l.get("geojson")
            if not gj or not gj.get("features"):
                continue
            for feat in gj["features"]:
                geom = feat.get("geometry", {})
                coords = geom.get("coordinates", [])
                geom_type = geom.get("type", "")
                if geom_type == "Point":
                    all_coords.append(coords)
                elif geom_type in ("LineString", "MultiPoint"):
                    all_coords.extend(coords)
                elif geom_type in ("Polygon", "MultiLineString"):
                    for ring in coords:
                        all_coords.extend(ring)
                elif geom_type == "MultiPolygon":
                    for poly in coords:
                        for ring in poly:
                            all_coords.extend(ring)

        if not all_coords:
            return None

        xs = [c[0] for c in all_coords if len(c) >= 2]
        ys = [c[1] for c in all_coords if len(c) >= 2]
        if not xs:
            return None

        xmin, xmax = min(xs), max(xs)
        ymin, ymax = min(ys), max(ys)
        return f"POLYGON(({xmin} {ymin},{xmax} {ymin},{xmax} {ymax},{xmin} {ymax},{xmin} {ymin}))"
    except Exception:
        return None
router = APIRouter(prefix="/api/maps", tags=["maps"])
MAX_PUBLIC = 10


# ── Schemas ───────────────────────────────────────────────────
class MapCreate(BaseModel):
    title:       str
    description: str  = ""
    state_json:  dict = {}
    thumbnail:   str  = ""
    is_public:   bool = False

class MapPatch(BaseModel):
    title:       str  | None = None
    description: str  | None = None
    state_json:  dict | None = None
    thumbnail:   str  | None = None
    is_public:   bool | None = None


# ── Helpers ───────────────────────────────────────────────────
def _validate_uuid(value: str, field: str = "id"):
    """Leve 400 si la valeur n'est pas un UUID valide (evite le DataError psycopg2)."""
    try:
        uuid.UUID(str(value))
    except (ValueError, AttributeError):
        raise HTTPException(400, f"{field} invalide : '{value}' n'est pas un UUID.")

def _to_text(d: dict) -> str:
    return json.dumps(d, ensure_ascii=False)

def _from_text(t) -> dict:
    if not t:
        return {}
    if isinstance(t, dict):
        return t
    try:
        return json.loads(t)
    except Exception:
        return {}

def _map_out(m: Map) -> dict:
    return {
        "id":          str(m.id),
        "title":       m.title,
        "description": m.description,
        "slug":        m.slug,
        "thumbnail":   m.thumbnail or "",
        "is_public":   m.is_public,
        "view_count":  m.view_count,
        "created_at":  m.created_at.isoformat(),
        "updated_at":  m.updated_at.isoformat(),
    }

def _map_full(m: Map) -> dict:
    d = _map_out(m)
    d["state_json"] = _from_text(m.state_json)
    d["bbox_wkt"]   = getattr(m, "bbox_wkt", None)  # None si colonne pas encore migrée
    return d

def _make_slug(title: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:60]
    return f"{base}-{uuid.uuid4().hex[:6]}"

def _count_public(db: Session, user_id) -> int:
    return db.query(Map).filter(Map.user_id == user_id, Map.is_public == True).count()


# ── Routes auth ───────────────────────────────────────────────
@router.get("")
def list_maps(
    page:    int     = Query(1, ge=1),
    limit:   int     = Query(20, le=50),
    current: User    = Depends(get_current_user),
    db:      Session = Depends(get_db),
):
    offset = (page - 1) * limit
    total  = db.query(Map).filter(Map.user_id == current.id).count()
    maps   = (db.query(Map).filter(Map.user_id == current.id)
              .order_by(desc(Map.updated_at)).offset(offset).limit(limit).all())
    return {
        "maps":         [_map_out(m) for m in maps],
        "total":        total,
        "public_count": _count_public(db, current.id),
        "public_limit": MAX_PUBLIC,
    }


@router.post("", status_code=201)
def create_map(
    body:    MapCreate,
    current: User    = Depends(get_current_user),
    db:      Session = Depends(get_db),
):
    if body.is_public and _count_public(db, current.id) >= MAX_PUBLIC:
        raise HTTPException(400, f"Limite : {MAX_PUBLIC} vignettes partageables max.")
    try:
        state_text = _to_text(body.state_json)
    except Exception as e:
        raise HTTPException(400, f"state_json invalide : {e}")
    try:
        bbox_wkt = _extract_bbox_wkt(body.state_json)
        m = Map(
            user_id=current.id, title=body.title, description=body.description,
            slug=_make_slug(body.title), state_json=state_text,
            thumbnail=body.thumbnail, is_public=body.is_public,
        )
        # bbox_wkt : seulement si la colonne existe (migration optionnelle)
        try:
            m.bbox_wkt = bbox_wkt
        except Exception:
            pass
        db.add(m); db.commit(); db.refresh(m)
    except Exception as e:
        db.rollback()
        log.error(f"create_map DB error: {e}")
        raise HTTPException(500, f"Erreur base de donnees : {e}")
    log.info(f"Carte creee : {m.title} ({m.slug}) par {current.email}")
    return _map_full(m)


@router.get("/my/{map_id}")
def get_map(
    map_id:  str,
    current: User    = Depends(get_current_user),
    db:      Session = Depends(get_db),
):
    _validate_uuid(map_id, "map_id")   # ← guard UUID
    m = db.query(Map).filter(Map.id == map_id, Map.user_id == current.id).first()
    if not m:
        raise HTTPException(404, "Carte introuvable")
    return _map_full(m)


@router.patch("/my/{map_id}")
def update_map(
    map_id:  str,
    body:    MapPatch,
    current: User    = Depends(get_current_user),
    db:      Session = Depends(get_db),
):
    _validate_uuid(map_id, "map_id")   # ← guard UUID — evite DataError "undefined"
    m = db.query(Map).filter(Map.id == map_id, Map.user_id == current.id).first()
    if not m:
        raise HTTPException(404, "Carte introuvable")

    if body.is_public is True and not m.is_public:
        if _count_public(db, current.id) >= MAX_PUBLIC:
            raise HTTPException(400, f"Limite : {MAX_PUBLIC} vignettes partageables max.")

    if body.title       is not None: m.title       = body.title
    if body.description is not None: m.description = body.description
    if body.thumbnail   is not None: m.thumbnail   = body.thumbnail
    if body.is_public   is not None: m.is_public   = body.is_public
    if body.state_json  is not None:
        try:
            m.state_json = _to_text(body.state_json)
            try:
                m.bbox_wkt = _extract_bbox_wkt(body.state_json)
            except Exception:
                pass
        except Exception as e:
            raise HTTPException(400, f"state_json invalide : {e}")
    try:
        db.commit(); db.refresh(m)
    except Exception as e:
        db.rollback()
        log.error(f"update_map DB error: {e}")
        raise HTTPException(500, f"Erreur base de donnees : {e}")
    return _map_full(m)


@router.delete("/my/{map_id}", status_code=204)
def delete_map(
    map_id:  str,
    current: User    = Depends(get_current_user),
    db:      Session = Depends(get_db),
):
    _validate_uuid(map_id, "map_id")
    m = db.query(Map).filter(Map.id == map_id, Map.user_id == current.id).first()
    if not m:
        raise HTTPException(404, "Carte introuvable")
    try:
        db.delete(m); db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(500, f"Erreur base de donnees : {e}")


# ── Routes publiques ──────────────────────────────────────────
@router.get("/share/{slug}")
def share_map(slug: str, db: Session = Depends(get_db)):
    m = db.query(Map).filter(Map.slug == slug, Map.is_public == True).first()
    if not m:
        raise HTTPException(404, "Carte introuvable ou non publique")
    m.view_count += 1
    db.commit()
    return _map_full(m)


@router.get("/gallery")
def gallery(
    page:  int     = Query(1, ge=1),
    limit: int     = Query(12, le=24),
    db:    Session = Depends(get_db),
):
    offset = (page - 1) * limit
    total  = db.query(Map).filter(Map.is_public == True).count()
    maps   = (db.query(Map).filter(Map.is_public == True)
              .order_by(desc(Map.view_count), desc(Map.updated_at))
              .offset(offset).limit(limit).all())
    return {"maps": [_map_out(m) for m in maps], "total": total}