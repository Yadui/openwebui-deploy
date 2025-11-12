import requests
from datetime import datetime, timedelta
from fastapi import HTTPException
from open_webui.models.users import Users
from open_webui.env import OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_TENANT_ID
import msal

POWERBI_SCOPES = ["https://analysis.windows.net/powerbi/api/.default"]


def get_powerbi_token_for_user(user):
    """Get a valid Power BI token (refresh if expired)."""
    if (
        user.powerbi_access_token
        and user.powerbi_expires_at
        and user.powerbi_expires_at > int(datetime.utcnow().timestamp())
    ):
        return user.powerbi_access_token

    if not user.powerbi_refresh_token:
        raise HTTPException(status_code=401, detail="Power BI token missing or expired")

    app = msal.ConfidentialClientApplication(
        OAUTH_CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{OAUTH_TENANT_ID}",
        client_credential=OAUTH_CLIENT_SECRET,
    )

    result = app.acquire_token_by_refresh_token(
        user.powerbi_refresh_token,
        scopes=POWERBI_SCOPES,
    )

    if "access_token" in result:
        access_token = result["access_token"]
        expires_at = int(
            (datetime.utcnow() + timedelta(seconds=result["expires_in"])).timestamp()
        )

        Users.update_user_powerbi_tokens(
            user.id,
            access_token,
            user.powerbi_refresh_token,
            datetime.utcfromtimestamp(expires_at),
        )
        return access_token

    raise HTTPException(status_code=401, detail="Unable to refresh Power BI token")


def powerbi_get(url: str, user, params=None):
    """Make a Power BI API GET request using the user's delegated token."""
    token = get_powerbi_token_for_user(user)
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()
