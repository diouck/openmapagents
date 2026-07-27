"""
auth/routes.py
POST /api/auth/register
POST /api/auth/login
POST /api/auth/refresh
GET  /api/auth/me
PATCH /api/auth/me
"""
import re
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr, field_validator

from .models import User
from .core import (
    get_db, get_current_user,
    hash_password, verify_password,
    create_access_token, create_refresh_token, decode_token,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])


# ─────────────────────────────────────────────────────────────
# Schémas Pydantic
# ─────────────────────────────────────────────────────────────
class RegisterIn(BaseModel):
    email:    EmailStr
    username: str
    password: str

    @field_validator("username")
    @classmethod
    def username_valid(cls, v):
        if not re.match(r"^[a-zA-Z0-9_\-]{3,30}$", v):
            raise ValueError("3-30 caractères, lettres/chiffres/_/- uniquement")
        return v

    @field_validator("password")
    @classmethod
    def pwd_strength(cls, v):
        if len(v) < 8:
            raise ValueError("Mot de passe trop court (min 8 caractères)")
        return v


class LoginIn(BaseModel):
    email:    EmailStr
    password: str


class RefreshIn(BaseModel):
    refresh_token: str


class ProfilePatch(BaseModel):
    username:     str | None = None
    password:     str | None = None
    old_password: str | None = None


def _user_out(u: User) -> dict:
    return {
        "id":         str(u.id),
        "email":      u.email,
        "username":   u.username,
        "is_admin":   u.is_admin,
        "created_at": u.created_at.isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────
@router.post("/register", status_code=201)
def register(body: RegisterIn, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(400, "Email déjà utilisé")
    if db.query(User).filter(User.username == body.username).first():
        raise HTTPException(400, "Nom d'utilisateur déjà pris")

    user = User(
        email    = body.email,
        username = body.username,
        pwd_hash = hash_password(body.password),
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        "user":          _user_out(user),
        "access_token":  create_access_token(str(user.id)),
        "refresh_token": create_refresh_token(str(user.id)),
        "token_type":    "bearer",
    }


@router.post("/login")
def login(body: LoginIn, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == body.email).first()
    if not user or not verify_password(body.password, user.pwd_hash):
        raise HTTPException(401, "Email ou mot de passe incorrect")
    if not user.is_active:
        raise HTTPException(403, "Compte désactivé")

    return {
        "user":          _user_out(user),
        "access_token":  create_access_token(str(user.id)),
        "refresh_token": create_refresh_token(str(user.id)),
        "token_type":    "bearer",
    }


@router.post("/refresh")
def refresh(body: RefreshIn, db: Session = Depends(get_db)):
    payload = decode_token(body.refresh_token)
    if payload.get("type") != "refresh":
        raise HTTPException(401, "Token de rafraîchissement invalide")
    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user or not user.is_active:
        raise HTTPException(401, "Utilisateur introuvable")

    return {
        "access_token":  create_access_token(str(user.id)),
        "refresh_token": create_refresh_token(str(user.id)),
        "token_type":    "bearer",
    }


@router.get("/me")
def me(current: User = Depends(get_current_user)):
    return _user_out(current)


# ─────────────────────────────────────────────────────────────
# Alias — compatibilité avec osm_routes, maps/routes, etc.
# qui importent `current_user` depuis auth.routes
# ─────────────────────────────────────────────────────────────
current_user = get_current_user


@router.patch("/me")
def update_me(
    body:    ProfilePatch,
    current: User    = Depends(get_current_user),
    db:      Session = Depends(get_db),
):
    if body.username and body.username != current.username:
        if db.query(User).filter(User.username == body.username).first():
            raise HTTPException(400, "Nom d'utilisateur déjà pris")
        current.username = body.username

    if body.password:
        if not body.old_password or not verify_password(body.old_password, current.pwd_hash):
            raise HTTPException(400, "Ancien mot de passe incorrect")
        if len(body.password) < 8:
            raise HTTPException(400, "Nouveau mot de passe trop court")
        current.pwd_hash = hash_password(body.password)

    db.commit()
    db.refresh(current)
    return _user_out(current)