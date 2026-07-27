"""
auth/routes.py — Endpoints FastAPI : register, login, refresh, me
"""
import logging
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from auth.core import (
    get_db, create_user, get_user_by_email, get_user_by_id,
    verify_password, create_access_token, create_refresh_token, decode_token
)

log    = logging.getLogger("auth")
router = APIRouter(prefix="/api/auth", tags=["auth"])
bearer = HTTPBearer(auto_error=False)

# ── Schémas ───────────────────────────────────────────────────
class RegisterIn(BaseModel):
    email:    EmailStr
    username: str
    password: str

class LoginIn(BaseModel):
    email:    EmailStr
    password: str

class RefreshIn(BaseModel):
    refresh_token: str

# ── Dépendance auth ───────────────────────────────────────────
def current_user(
    creds: HTTPAuthorizationCredentials = Depends(bearer),
    db:    Session = Depends(get_db),
):
    if not creds:
        raise HTTPException(401, "Token manquant")
    payload = decode_token(creds.credentials)
    if not payload or payload.get("type") != "access":
        raise HTTPException(401, "Token invalide ou expiré")
    user = get_user_by_id(db, payload["sub"])
    if not user or not user.is_active:
        raise HTTPException(401, "Utilisateur introuvable")
    return user

# ── Endpoints ─────────────────────────────────────────────────
@router.post("/register")
def register(body: RegisterIn, db: Session = Depends(get_db)):
    if get_user_by_email(db, body.email):
        raise HTTPException(400, "Email déjà utilisé")
    if len(body.password) < 8:
        raise HTTPException(400, "Mot de passe trop court (min 8 caractères)")
    if len(body.username) < 3:
        raise HTTPException(400, "Nom d'utilisateur trop court (min 3 caractères)")
    user = create_user(db, body.email, body.username, body.password)
    log.info(f"Nouvel utilisateur : {user.email}")
    return {
        "access_token":  create_access_token(str(user.id)),
        "refresh_token": create_refresh_token(str(user.id)),
        "user": {"id": str(user.id), "email": user.email, "username": user.username},
    }

@router.post("/login")
def login(body: LoginIn, db: Session = Depends(get_db)):
    user = get_user_by_email(db, body.email)
    if not user or not verify_password(body.password, user.pwd_hash):
        raise HTTPException(401, "Email ou mot de passe incorrect")
    if not user.is_active:
        raise HTTPException(403, "Compte désactivé")
    return {
        "access_token":  create_access_token(str(user.id)),
        "refresh_token": create_refresh_token(str(user.id)),
        "user": {"id": str(user.id), "email": user.email, "username": user.username},
    }

@router.post("/refresh")
def refresh(body: RefreshIn, db: Session = Depends(get_db)):
    payload = decode_token(body.refresh_token)
    if not payload or payload.get("type") != "refresh":
        raise HTTPException(401, "Refresh token invalide")
    user = get_user_by_id(db, payload["sub"])
    if not user or not user.is_active:
        raise HTTPException(401, "Utilisateur introuvable")
    return {
        "access_token":  create_access_token(str(user.id)),
        "refresh_token": create_refresh_token(str(user.id)),
    }

@router.get("/me")
def me(user=Depends(current_user)):
    return {"id": str(user.id), "email": user.email, "username": user.username,
            "created_at": user.created_at.isoformat()}

@router.patch("/me")
def update_me(body: dict, user=Depends(current_user), db: Session = Depends(get_db)):
    if "username" in body and len(body["username"]) >= 3:
        user.username = body["username"]
    if "password" in body and len(body["password"]) >= 8:
        from auth.core import hash_password
        user.pwd_hash = hash_password(body["password"])
    db.commit()
    return {"id": str(user.id), "email": user.email, "username": user.username}
