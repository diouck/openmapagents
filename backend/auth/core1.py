"""
auth/core.py — JWT + bcrypt + DB session
"""
import os, uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

from jose import jwt, JWTError
from passlib.context import CryptContext
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from auth.models import Base, User, Map

# ── Config ────────────────────────────────────────────────────
SECRET_KEY    = os.getenv("JWT_SECRET", "changeme-generate-with-openssl-rand-hex-32")
ALGORITHM     = "HS256"
ACCESS_EXP    = int(os.getenv("JWT_ACCESS_MINUTES", "60"))      # 1h
REFRESH_EXP   = int(os.getenv("JWT_REFRESH_DAYS",   "7"))       # 7j

DATABASE_URL  = (
    f"postgresql://{os.getenv('PG_USER','postgres')}:{os.getenv('PG_PASSWORD','')}"
    f"@{os.getenv('PG_HOST','geoafrica.fr')}:{os.getenv('PG_PORT','5435')}"
    f"/{os.getenv('PG_DB','openmapagents')}"
)

engine       = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
pwd_ctx      = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ── Init DB ───────────────────────────────────────────────────
def init_db():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ── Password ──────────────────────────────────────────────────
def hash_password(pwd: str) -> str:
    return pwd_ctx.hash(pwd)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)

# ── JWT ───────────────────────────────────────────────────────
def create_access_token(user_id: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_EXP)
    return jwt.encode({"sub": user_id, "type": "access", "exp": exp}, SECRET_KEY, ALGORITHM)

def create_refresh_token(user_id: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(days=REFRESH_EXP)
    return jwt.encode({"sub": user_id, "type": "refresh", "exp": exp}, SECRET_KEY, ALGORITHM)

def decode_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None

# ── User helpers ──────────────────────────────────────────────
def get_user_by_email(db: Session, email: str) -> Optional[User]:
    return db.query(User).filter(User.email == email.lower()).first()

def get_user_by_id(db: Session, user_id: str) -> Optional[User]:
    return db.query(User).filter(User.id == user_id).first()

def create_user(db: Session, email: str, username: str, password: str) -> User:
    user = User(email=email.lower(), username=username, pwd_hash=hash_password(password))
    db.add(user); db.commit(); db.refresh(user)
    return user

# ── Slug ──────────────────────────────────────────────────────
def generate_slug(title: str) -> str:
    import re
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:50]
    return f"{slug}-{uuid.uuid4().hex[:6]}"
