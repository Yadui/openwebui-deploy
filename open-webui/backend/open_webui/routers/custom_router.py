from fastapi import APIRouter, HTTPException, Request, Response
import httpx
import requests, os, logging
from msal import ConfidentialClientApplication

router = APIRouter()

TENANT_ID = os.getenv("MICROSOFT_CLIENT_TENANT_ID")
CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID")
WORKSPACE_ID = os.getenv("POWERBI_WORKSPACE_ID")
CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET")
POWERBI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]
logger = logging.getLogger("webui_powerbi")

CHARTING_URL = os.getenv("CHARTING_URL")


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


def get_obo_token(user_token: str) -> str:
    if not user_token or len(user_token) < 50:
        logger.warning(
            "No valid user token detected, using client_credentials fallback."
        )
        authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        app = ConfidentialClientApplication(
            CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
        )
        result = app.acquire_token_for_client(scopes=POWERBI_SCOPE)
        if "access_token" not in result:
            raise HTTPException(status_code=401, detail="Failed to get app token.")
        return result["access_token"]
    logger.info("Attempting On-Behalf-Of token acquisition.")
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(
        client_id=CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )

    # Define the specific scopes needed for OBO with Power BI
    specific_scopes = ["https://analysis.windows.net/powerbi/api/.default"]

    logger.debug(f"Requesting OBO token with scopes: {specific_scopes}")

    # Truncate token for logging security
    user_token_display = (
        f"{user_token[:10]}...{user_token[-4:]}" if len(user_token) > 14 else user_token
    )
    logger.debug(f"Using user assertion (token starting with): {user_token_display}")
    # Try refreshing if expired
    user_token = refresh_microsoft_token(user_token)

    result = app.acquire_token_on_behalf_of(
        user_assertion=user_token, scopes=specific_scopes
    )

    if "access_token" in result:
        logger.info("OBO token acquisition successful.")
        # Avoid logging the full token itself for security
        logger.debug("OBO token acquired.")
        return result["access_token"]
    else:
        # Log the detailed error from Microsoft Entra ID
        error_code = result.get("error")
        error_description = result.get("error_description")
        correlation_id = result.get("correlation_id")
        logger.error(
            f"OBO flow failed: ErrorCode={error_code}, Description='{error_description}', CorrelationID={correlation_id}"
        )
        # Log the scopes requested again for context
        logger.error(f"Failed scopes: {specific_scopes}")
        raise HTTPException(
            status_code=401,
            detail=f"Could not acquire token on behalf of user. Error: {error_description}",
        )
        clean_msg = result["error_description"].split("Trace ID")[0]
        raise HTTPException(
            status_code=401, detail=f"OBO Token Error: {clean_msg.strip()}"
        )


def list_datasets_in_workspace(token: str):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets"
    headers = {"Authorization": f"Bearer {token}"}
    logger.info(f"Listing datasets in workspace: {WORKSPACE_ID}")
    logger.debug(f"Request URL: {url}")
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()  # Check for HTTP errors (like 401, 403, 404)
        datasets = resp.json().get("value", [])
        logger.info(f"Found {len(datasets)} datasets.")
        return [{"name": ds["name"], "id": ds["id"]} for ds in datasets]
    except requests.exceptions.RequestException as e:
        logger.error(f"Power BI API request failed for listing datasets: {e}")
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
    logger.info("Listing workspaces.")
    logger.debug(f"Request URL: {url}")
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        workspaces = resp.json().get("value", [])
        logger.info(f"Found {len(workspaces)} workspaces.")
        return [ws["name"] for ws in workspaces]
    except requests.exceptions.RequestException as e:
        logger.error(f"Power BI API request failed for listing workspaces: {e}")
        status_code = (
            e.response.status_code
            if hasattr(e, "response") and e.response is not None
            else 500
        )
        detail = f"Power BI API Error: {str(e)}"
        raise HTTPException(status_code=status_code, detail=detail) from e


@router.get("/powerbi/workspaces")
def list_workspaces_api(request: Request):
    # Try to extract the *real* Microsoft Entra token (set by OpenWebUI after Microsoft login)
    auth_header = request.headers.get("Authorization")
    ms_oauth_token = (
        request.cookies.get("oauth_id_token")
        or request.cookies.get("microsoft_token")
        or request.cookies.get("id_token")
    )

    user_token = None

    if auth_header and auth_header.startswith("Bearer "):
        user_token = auth_header.split(" ")[1]
    elif ms_oauth_token:
        user_token = ms_oauth_token
    else:
        logger.error("No valid Microsoft Entra token found in request.")
        raise HTTPException(status_code=401, detail="Missing Microsoft Entra token.")

    # 2️⃣ Exchange for OBO token
    powerbi_token = get_obo_token(user_token)

    # 3️⃣ Call Power BI API using the same helper
    workspaces = list_workspaces(powerbi_token)
    return [{"name": ws} for ws in workspaces]


@router.get("/powerbi/workspaces/{workspace_id}/datasets")
def list_datasets_api(workspace_id: str, request: Request):
    auth_header = request.headers.get("Authorization")
    ms_oauth_token = (
        request.cookies.get("oauth_id_token")
        or request.cookies.get("microsoft_token")
        or request.cookies.get("id_token")
    )

    user_token = None

    if auth_header and auth_header.startswith("Bearer "):
        user_token = auth_header.split(" ")[1]
    elif ms_oauth_token:
        user_token = ms_oauth_token
    else:
        logger.error("No valid Microsoft Entra token found in request.")
        raise HTTPException(status_code=401, detail="Missing Microsoft Entra token.")

    powerbi_token = get_obo_token(user_token)
    datasets = list_datasets_in_workspace(powerbi_token)
    return datasets


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
