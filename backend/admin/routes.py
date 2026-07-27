"""
admin/routes.py — Stats et gestion utilisateurs (is_admin requis)
GET  /api/admin/stats          → KPIs globaux
GET  /api/admin/users          → liste users paginée
PATCH /api/admin/users/{id}    → toggle is_active / is_admin
DELETE /api/admin/users/{id}   → supprimer user
GET  /api/admin/users/{id}/activity → activité détaillée
"""
import logging
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from pydantic import BaseModel

from auth.core import get_db, get_current_user
from auth.models import User, Map

log = logging.getLogger("admin")
router = APIRouter(prefix="/api/admin", tags=["admin"])


def require_admin(current: User = Depends(get_current_user)) -> User:
    if not current.is_admin:
        raise HTTPException(403, "Accès administrateur requis")
    return current


def _user_out(u: User, map_count: int = 0) -> dict:
    return {
        "id":         str(u.id),
        "email":      u.email,
        "username":   u.username,
        "is_active":  u.is_active,
        "is_admin":   u.is_admin,
        "map_count":  map_count,
        "created_at": u.created_at.isoformat(),
        "updated_at": u.updated_at.isoformat(),
    }


# ── Stats globales ─────────────────────────────────────────────
@router.get("/stats")
def admin_stats(
    admin: User    = Depends(require_admin),
    db:    Session = Depends(get_db),
):
    now   = datetime.now(timezone.utc)
    day7  = now - timedelta(days=7)
    day30 = now - timedelta(days=30)

    total_users   = db.query(func.count(User.id)).scalar()
    active_users  = db.query(func.count(User.id)).filter(User.is_active == True).scalar()
    admin_users   = db.query(func.count(User.id)).filter(User.is_admin == True).scalar()
    new_7d        = db.query(func.count(User.id)).filter(User.created_at >= day7).scalar()
    new_30d       = db.query(func.count(User.id)).filter(User.created_at >= day30).scalar()

    total_maps    = db.query(func.count(Map.id)).scalar()
    public_maps   = db.query(func.count(Map.id)).filter(Map.is_public == True).scalar()
    maps_7d       = db.query(func.count(Map.id)).filter(Map.created_at >= day7).scalar()
    total_views   = db.query(func.coalesce(func.sum(Map.view_count), 0)).scalar()

    # Top 5 users par nb de cartes
    top_users = (
        db.query(User.username, User.email, func.count(Map.id).label("maps"))
        .outerjoin(Map, Map.user_id == User.id)
        .group_by(User.id)
        .order_by(desc("maps"))
        .limit(5).all()
    )

    # Inscriptions par jour (30 derniers jours)
    signups = (
        db.query(
            func.date_trunc("day", User.created_at).label("day"),
            func.count(User.id).label("count"),
        )
        .filter(User.created_at >= day30)
        .group_by("day")
        .order_by("day")
        .all()
    )

    return {
        "users": {
            "total": total_users, "active": active_users,
            "admin": admin_users, "new_7d": new_7d, "new_30d": new_30d,
        },
        "maps": {
            "total": total_maps, "public": public_maps,
            "new_7d": maps_7d, "total_views": int(total_views),
        },
        "top_users": [
            {"username": u.username, "email": u.email, "maps": u.maps}
            for u in top_users
        ],
        "signups_30d": [
            {"day": s.day.strftime("%Y-%m-%d"), "count": s.count}
            for s in signups
        ],
    }


# ── Liste users ────────────────────────────────────────────────
@router.get("/users")
def admin_list_users(
    page:   int = Query(1, ge=1),
    limit:  int = Query(20, le=100),
    search: str = Query(""),
    admin:  User    = Depends(require_admin),
    db:     Session = Depends(get_db),
):
    q = db.query(User)
    if search:
        q = q.filter(
            (User.email.ilike(f"%{search}%")) |
            (User.username.ilike(f"%{search}%"))
        )
    total  = q.count()
    users  = q.order_by(desc(User.created_at)).offset((page-1)*limit).limit(limit).all()

    # Compter les cartes par user en une requête
    ids = [u.id for u in users]
    counts = dict(
        db.query(Map.user_id, func.count(Map.id))
        .filter(Map.user_id.in_(ids))
        .group_by(Map.user_id).all()
    )

    return {
        "users": [_user_out(u, counts.get(u.id, 0)) for u in users],
        "total": total, "page": page, "limit": limit,
    }


# ── Patch user (toggle active/admin) ──────────────────────────
class UserPatch(BaseModel):
    is_active: bool | None = None
    is_admin:  bool | None = None

@router.patch("/users/{user_id}")
def admin_patch_user(
    user_id: str,
    body:    UserPatch,
    admin:   User    = Depends(require_admin),
    db:      Session = Depends(get_db),
):
    if str(user_id) == str(admin.id):
        raise HTTPException(400, "Impossible de se modifier soi-même")
    u = db.query(User).filter(User.id == user_id).first()
    if not u: raise HTTPException(404, "Utilisateur introuvable")
    if body.is_active is not None: u.is_active = body.is_active
    if body.is_admin  is not None: u.is_admin  = body.is_admin
    db.commit(); db.refresh(u)
    counts = db.query(func.count(Map.id)).filter(Map.user_id == u.id).scalar()
    return _user_out(u, counts)


# ── Supprimer user ─────────────────────────────────────────────
@router.delete("/users/{user_id}", status_code=204)
def admin_delete_user(
    user_id: str,
    admin:   User    = Depends(require_admin),
    db:      Session = Depends(get_db),
):
    if str(user_id) == str(admin.id):
        raise HTTPException(400, "Impossible de se supprimer soi-même")
    u = db.query(User).filter(User.id == user_id).first()
    if not u: raise HTTPException(404, "Utilisateur introuvable")
    db.delete(u); db.commit()


# ── Activité d'un user ─────────────────────────────────────────
@router.get("/users/{user_id}/activity")
def admin_user_activity(
    user_id: str,
    admin:   User    = Depends(require_admin),
    db:      Session = Depends(get_db),
):
    u = db.query(User).filter(User.id == user_id).first()
    if not u: raise HTTPException(404, "Utilisateur introuvable")

    maps = (db.query(Map).filter(Map.user_id == user_id)
            .order_by(desc(Map.updated_at)).limit(20).all())

    return {
        "user": _user_out(u),
        "maps": [{
            "id":         str(m.id),
            "title":      m.title,
            "is_public":  m.is_public,
            "view_count": m.view_count,
            "created_at": m.created_at.isoformat(),
            "updated_at": m.updated_at.isoformat(),
        } for m in maps],
    }


# ── À ajouter dans agent.py ───────────────────────────────────
# try:
#     from admin.routes import router as admin_router
#     app.include_router(admin_router)
#     log.info("✓ Admin router chargé (/api/admin/*)")
# except Exception as e:
#     log.warning(f"⚠ Admin router : {e}")


# ── Route admin/maps — toutes les cartes de tous les users ────
@router.get("/maps")
def admin_list_maps(
    page:   int = Query(1, ge=1),
    limit:  int = Query(20, le=100),
    search: str = Query(""),
    admin:  User    = Depends(require_admin),
    db:     Session = Depends(get_db),
):
    """Liste toutes les cartes (admin only) avec info propriétaire."""
    from sqlalchemy import or_

    q = db.query(Map, User.username).join(User, Map.user_id == User.id)
    if search:
        q = q.filter(
            or_(
                Map.title.ilike(f"%{search}%"),
                User.username.ilike(f"%{search}%"),
            )
        )
    total = q.count()
    rows  = q.order_by(desc(Map.updated_at)).offset((page-1)*limit).limit(limit).all()

    maps = []
    for m, username in rows:
        d = {
            "id":          str(m.id),
            "title":       m.title,
            "description": m.description,
            "slug":        m.slug,
            "thumbnail":   m.thumbnail or "",
            "is_public":   m.is_public,
            "view_count":  m.view_count,
            "username":    username,
            "created_at":  m.created_at.isoformat(),
            "updated_at":  m.updated_at.isoformat(),
        }
        maps.append(d)

    return {"maps": maps, "total": total, "page": page, "limit": limit}