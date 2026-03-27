# backend/open_webui/routers/powerbi_query.py
from fastapi import APIRouter, Request, HTTPException, Depends
from pydantic import BaseModel
import httpx
from open_webui.utils.auth import get_verified_user
import logging
import aiohttp

log = logging.getLogger(__name__)

router = APIRouter()


class PowerBIQueryForm(BaseModel):
    workspace_id: str
    dataset_id: str
    dax_query: str


async def execute_dax_query(
    workspace_id: str, dataset_id: str, dax_query: str, access_token: str
):
    """
    Executes a DAX query against a Power BI dataset using the REST API.

    Args:
        workspace_id (str): The Power BI workspace (group) ID.
        dataset_id (str): The dataset ID within the workspace.
        dax_query (str): The DAX query to execute.
        access_token (str): User's Entra access token for authorization.

    Returns:
        dict: Parsed results from Power BI or error info.
    """

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    payload = {"queries": [{"query": dax_query}]}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    log.error(f"❌ Power BI query failed: {resp.status} - {text}")
                    return {"status": "error", "message": text, "data": []}

                data = await resp.json()
                tables = data.get("results", [{}])[0].get("tables", [])
                if not tables:
                    return {"status": "success", "data": []}

                rows = tables[0].get("rows", [])
                columns = tables[0].get("columns", [])
                col_names = [col["name"] for col in columns]

                formatted = [
                    {col_names[i]: row[i] for i in range(len(col_names))}
                    for row in rows
                ]
                return {"status": "success", "data": formatted}
        except Exception as e:
            log.exception(f"⚠️ Power BI DAX execution failed: {e}")
            return {"status": "error", "message": str(e), "data": []}
