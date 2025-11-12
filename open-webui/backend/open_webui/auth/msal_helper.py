import os, time, json, logging
from pathlib import Path
from msal import ConfidentialClientApplication
from fastapi import Request, HTTPException
import jwt

logger = logging.getLogger("msal_helper")

TENANT_ID = os.getenv("MICROSOFT_CLIENT_TENANT_ID")
CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID")
CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET")
POWERBI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]

# Primary Azure Files mount cache
AZURE_CACHE_FILE = Path("/app/backend/data/powerbi_token_cache.json")

# Local fallback cache for dev environments
LOCAL_CACHE_FILE = Path("./tools/cache/powerbi_token_cache.json")

# ---------- CACHE HANDLING ----------


def _load_cache():
    for path in [AZURE_CACHE_FILE, LOCAL_CACHE_FILE]:
        if path.exists():
            try:
                data = json.loads(path.read_text())
                logger.debug(f"Loaded cache from {path}")
                return data
            except Exception as e:
                logger.warning(f"Failed to read cache from {path}: {e}")
    return {}


def _save_cache(cache):
    for path in [AZURE_CACHE_FILE, LOCAL_CACHE_FILE]:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(cache, indent=2))
            logger.debug(f"Saved Power BI token cache to {path}")
            return
        except Exception as e:
            logger.warning(f"Failed to save cache to {path}: {e}")
    logger.error("❌ Could not save token cache to any location.")


def _is_valid(entry):
    return entry and entry.get("expires_at", 0) > time.time() + 60


# ---------- TOKEN HANDLING ----------


def _obo_exchange(user_assertion):
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(
        client_id=CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_on_behalf_of(
        user_assertion=user_assertion, scopes=POWERBI_SCOPE
    )
    if "access_token" not in result:
        error = result.get("error_description", "Unknown OBO failure")
        logger.error(f"OBO exchange failed: {error}")
        raise HTTPException(status_code=401, detail=f"OBO exchange failed: {error}")
    return result["access_token"], result["expires_in"]


def _client_token():
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(
        CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=POWERBI_SCOPE)
    if "access_token" not in result:
        raise HTTPException(status_code=401, detail="Failed to get app-only token.")
    return result["access_token"], result["expires_in"]


# ---------- PUBLIC API ----------


def get_user_powerbi_token(request: Request) -> str:
    """Return a valid Power BI access token for this user or fallback to SP."""
    cookies = request.cookies
    ms_token = (
        cookies.get("oauth_id_token")
        or cookies.get("id_token")
        or cookies.get("microsoft_token")
    )
    if not ms_token:
        raise HTTPException(status_code=401, detail="Missing Microsoft login cookie.")

    # Decode username to use as cache key
    try:
        decoded = jwt.decode(ms_token, options={"verify_signature": False})
        username = decoded.get("preferred_username", "service")
    except Exception:
        username = "service"

    cache = _load_cache()
    entry = cache.get(username)

    if _is_valid(entry):
        logger.debug(f"Using cached Power BI token for {username}")
        return entry["access_token"]

    try:
        access_token, ttl = _obo_exchange(ms_token)
        logger.info(f"OBO success for {username}")
    except HTTPException:
        logger.warning(f"OBO failed for {username}, falling back to service principal")
        access_token, ttl = _client_token()

    cache[username] = {"access_token": access_token, "expires_at": time.time() + ttl}
    _save_cache(cache)
    return access_token
