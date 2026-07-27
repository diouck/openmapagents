"""
auth/core.py
— Connexion PostgreSQL via SQLAlchemy
— Helpers JWT (access 1 h + refresh 7 j)
— Dependency FastAPI : get_db, get_current_user
"""
import os
from datetime import datetime, timedelta, timezone

from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from .models import Base, User

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
# Priorité : DATABASE_URL dans .env, sinon construit depuis les variables
_DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
        user=os.getenv("PG_USER",     "postgres"),
        pwd =os.getenv("PG_PASSWORD", ""),
        host=os.getenv("PG_HOST",     "geoafrica.fr"),
        port=os.getenv("PG_PORT",     "5435"),
        db  =os.getenv("PG_DB",       "openmapagents"),
    ),
)

JWT_SECRET    = os.getenv("JWT_SECRET", "CHANGE_ME_openssl_rand_hex_32")
JWT_ALGORITHM = "HS256"
ACCESS_EXP    = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES",  "60"))
REFRESH_EXP   = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS",    "7"))

# ─────────────────────────────────────────────────────────────
# SQLAlchemy engine  (pool_pre_ping = reconnexion auto)
# ─────────────────────────────────────────────────────────────
engine = create_engine(
    _DB_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    connect_args={"connect_timeout": 10},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Crée les tables si elles n'existent pas encore."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """FastAPI dependency — session PostgreSQL."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────
# Bcrypt
# ─────────────────────────────────────────────────────────────
_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    return _pwd.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return _pwd.verify(plain, hashed)


# ─────────────────────────────────────────────────────────────
# JWT
# ─────────────────────────────────────────────────────────────
def _make_token(data: dict, expires_delta: timedelta) -> str:
    payload = data.copy()
    payload["exp"] = datetime.now(timezone.utc) + expires_delta
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def create_access_token(user_id: str) -> str:
    return _make_token(
        {"sub": user_id, "type": "access"},
        timedelta(minutes=ACCESS_EXP),
    )


def create_refresh_token(user_id: str) -> str:
    return _make_token(
        {"sub": user_id, "type": "refresh"},
        timedelta(days=REFRESH_EXP),
    )


def decode_token(token: str) -> dict:
    """Lève HTTPException 401 si invalide / expiré."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalide ou expiré",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ─────────────────────────────────────────────────────────────
# FastAPI dependency — utilisateur courant
# ─────────────────────────────────────────────────────────────
_bearer = HTTPBearer(auto_error=False)


def get_current_user(
    creds: HTTPAuthorizationCredentials = Depends(_bearer),
    db:    Session                       = Depends(get_db),
) -> User:
    if not creds:
        raise HTTPException(status_code=401, detail="Non authentifié")
    payload = decode_token(creds.credentials)
    if payload.get("type") != "access":
        raise HTTPException(status_code=401, detail="Token de type incorrect")
    user = db.query(User).filter(User.id == payload["sub"]).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="Utilisateur introuvable")
    return user
