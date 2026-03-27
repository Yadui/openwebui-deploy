import requests
from fastapi import HTTPException

POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"


async def execute_dax_query(
    workspace_id: str, dataset_id: str, dax_query: str, access_token: str
):
    """
    Execute a DAX query against a Power BI dataset using the user's delegated access token.
    """
    url = (
        f"{POWERBI_API_BASE}/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {"queries": [{"query": dax_query}]}

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def powerbi_get(url: str, access_token: str, params=None):
    """
    Perform a Power BI GET request using a delegated access token.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()
