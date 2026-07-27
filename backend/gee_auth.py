"""
gee_auth.py — Initialisation GEE unifiée Windows / Linux
Importer dans gee_routes.py, gee_change_detection.py, gee_timelapse.py :

    from gee_auth import init_gee, get_ee

Stratégie :
  1. Service account Linux (/var/www/google/...) — prod serveur
  2. Credentials OAuth locaux (~/.config/earthengine/credentials) — dev Windows/Linux
  3. Application Default Credentials — fallback
"""

import os
import logging

log = logging.getLogger("gee_auth")

# Chemin service account (prod Linux)
SA_EMAIL   = os.getenv("GEE_SA_EMAIL",   "mcpopenmapagents@laravelauth-477918.iam.gserviceaccount.com")
SA_KEYFILE = os.getenv("GEE_SA_KEYFILE", "/var/www/google/laravelauth-477918-9f353bf03d0b.json")
GEE_PROJECT = os.getenv("GEE_PROJECT",   "laravelauth-477918")

_gee_ready = False


def init_gee() -> bool:
    """
    Initialise GEE avec la meilleure méthode disponible.
    Retourne True si succès, False sinon.
    """
    global _gee_ready
    if _gee_ready:
        return True

    import ee

    # ── Méthode 1 : Service account Linux (prod) ──────────────────────────────
    if os.path.exists(SA_KEYFILE):
        try:
            credentials = ee.ServiceAccountCredentials(
                email=SA_EMAIL,
                key_file=SA_KEYFILE,
            )
            ee.Initialize(credentials)  # sans project= : evite le 403 IAM sur le serveur
            _gee_ready = True
            log.info(f"✓ GEE initialisé via service account ({SA_KEYFILE})")
            return True
        except Exception as e:
            log.warning(f"Service account GEE échoué : {e}")

    # ── Méthode 2 : OAuth credentials locaux (dev Windows/Linux) ─────────────
    cred_path = os.path.expanduser("~/.config/earthengine/credentials")
    if os.path.exists(cred_path):
        try:
            import json
            from ee import oauth as ee_oauth
            from google.oauth2.credentials import Credentials
            from google.auth.transport.requests import Request

            with open(cred_path) as f:
                raw = json.load(f)

            creds = Credentials(
                token=None,
                refresh_token=raw["refresh_token"],
                client_id=ee_oauth.CLIENT_ID,
                client_secret=ee_oauth.CLIENT_SECRET,
                token_uri=ee_oauth.TOKEN_URI,
                scopes=ee_oauth.SCOPES,
            )
            creds.refresh(Request())
            ee.Initialize(credentials=creds, project=GEE_PROJECT)
            _gee_ready = True
            log.info(f"✓ GEE initialisé via OAuth credentials ({cred_path})")
            return True
        except Exception as e:
            log.warning(f"OAuth GEE échoué : {e}")

    # ── Méthode 3 : Application Default Credentials ───────────────────────────
    try:
        ee.Initialize(project=GEE_PROJECT)
        _gee_ready = True
        log.info("✓ GEE initialisé via Application Default Credentials")
        return True
    except Exception as e:
        log.error(f"✗ GEE init totalement échoué : {e}")
        return False


def get_ee():
    """
    Retourne le module ee initialisé.
    Lève une exception si GEE n'est pas disponible.
    """
    import ee
    if not _gee_ready:
        if not init_gee():
            raise RuntimeError(
                "GEE non disponible. Vérifiez :\n"
                f"  - Service account : {SA_KEYFILE}\n"
                f"  - OAuth local     : ~/.config/earthengine/credentials\n"
                "  - Ou lancez : earthengine authenticate"
            )
    return ee


def reset():
    """Reset l'état GEE (utile pour les tests)."""
    global _gee_ready
    _gee_ready = False
