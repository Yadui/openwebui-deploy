from fastapi import APIRouter, HTTPException, Request, Response
import httpx
import requests, os, logging
from msal import ConfidentialClientApplication
import jwt
from open_webui.auth.msal_helper import get_user_powerbi_token
import time

router = APIRouter()

TENANT_ID = os.getenv("MICROSOFT_CLIENT_TENANT_ID")
CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID")
WORKSPACE_ID = os.getenv("POWERBI_WORKSPACE_ID")
CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET")
POWERBI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
logger = logging.getLogger("webui_powerbi")

CHARTING_URL = os.getenv("CHARTING_URL")

# Simple cache for workspaces with 10-minute expiration
workspace_cache = {
    "data": None,
    "timestamp": 0,
    "ttl": 600,  # 10 minutes in seconds
}


# ======================
# HELPER FUNCTIONS
# ======================
def refresh_microsoft_token(user_token: str):
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    token_endpoint = f"{authority}/oauth2/v2.0/token"

    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": user_token,
        "scope": "https://graph.microsoft.com/.default",
    }

    resp = requests.post(token_endpoint, data=data)
    if resp.status_code != 200:
        logger.warning(f"Refresh failed: {resp.text}")
        return user_token  # fallback to original
    new_token = resp.json().get("access_token")
    logger.info("✅ Microsoft access token refreshed successfully.")
    return new_token


@router.get("/powerbi/workspaces")
def list_workspaces_api(request: Request):
    logger.info("🟡 /powerbi/workspaces called.")
    logger.info(f"🔍 Cookies received: {list(request.cookies.keys())}")

    current_time = time.time()
    if (
        workspace_cache["data"] is not None
        and current_time - workspace_cache["timestamp"] < workspace_cache["ttl"]
    ):
        logger.info("Returning cached workspaces data.")
        return workspace_cache["data"]

    try:
        token = get_user_powerbi_token(request)
        logger.info("✅ Got Power BI token from request.")
    except Exception as e:
        logger.error(f"❌ get_user_powerbi_token failed: {e}")
        raise HTTPException(
            status_code=401, detail=f"Token extraction failed: {str(e)}"
        )

    logger.info("Fetching workspaces from Power BI API.")
    try:
        workspaces = list_workspaces(token)
        workspace_cache["data"] = workspaces
        workspace_cache["timestamp"] = current_time
        logger.info(f"✅ Cached {len(workspaces)} workspaces for 10 minutes.")
        return workspaces
    except Exception as e:
        logger.error(f"❌ Failed to fetch workspaces: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch workspaces: {str(e)}"
        )


def is_id_token(token: str) -> bool:
    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        # Check for Entra ID issuer and ID token claim
        return "iss" in decoded and "aud" in decoded and "preferred_username" in decoded
    except Exception:
        return False


def list_datasets_in_workspace(token: str, workspace_id: str):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    headers = {"Authorization": f"Bearer {token}"}
    logger.info(f"Listing datasets in workspace: {workspace_id}")
    logger.debug(f"Request URL: {url}")
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()  # Check for HTTP errors (like 401, 403, 404)
        datasets = resp.json().get("value", [])
        logger.info(f"Found {len(datasets)} datasets in workspace {workspace_id}.")
        return [{"name": ds["name"], "id": ds["id"]} for ds in datasets]
    except requests.exceptions.RequestException as e:
        logger.error(
            f"Power BI API request failed for listing datasets in workspace {workspace_id}: {e}"
        )
        # Re-raise as HTTPException to send error to client
        status_code = (
            e.response.status_code
            if hasattr(e, "response") and e.response is not None
            else 500
        )
        detail = f"Power BI API Error: {str(e)}"
        raise HTTPException(status_code=status_code, detail=detail) from e


def list_workspaces(token: str):
    url = "https://api.powerbi.com/v1.0/myorg/groups"
    headers = {"Authorization": f"Bearer {token}"}
    logger.info("Listing workspaces from Power BI API.")
    logger.debug(f"Request URL: {url}")
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        workspaces = resp.json().get("value", [])
        logger.info(f"Found {len(workspaces)} workspaces.")
        return [{"name": ws["name"], "id": ws["id"]} for ws in workspaces]
    except requests.exceptions.RequestException as e:
        logger.error(f"Power BI API request failed for listing workspaces: {e}")
        status_code = (
            e.response.status_code
            if hasattr(e, "response") and e.response is not None
            else 500
        )
        detail = f"Power BI API Error: {str(e)}"
        raise HTTPException(status_code=status_code, detail=detail) from e


@router.get("/powerbi/workspaces/{workspace_id}/datasets")
def list_datasets_api(workspace_id: str, request: Request):
    token = get_user_powerbi_token(request)
    return list_datasets_in_workspace(token, workspace_id)


@router.api_route("/powerbi/{path:path}", methods=["GET", "POST"])
async def proxy_to_charting(request: Request, path: str):
    """Proxy Power BI API calls to the charting backend, preserving cookies."""
    async with httpx.AsyncClient() as client:
        # Forward the incoming request to the charting service
        charting_resp = await client.request(
            method=request.method,
            url=f"{CHARTING_URL}/{path}",
            headers=request.headers,
            cookies=request.cookies,
            content=await request.body(),
        )

    # Return the response back to frontend
    return Response(
        content=charting_resp.content,
        status_code=charting_resp.status_code,
        headers=dict(charting_resp.headers),
    )
