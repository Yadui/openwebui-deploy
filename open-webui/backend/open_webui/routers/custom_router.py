import httpx
import asyncio
import os
import logging
import time
from fastapi import APIRouter, HTTPException, Request, Response, Depends
from fastapi.concurrency import run_in_threadpool
from open_webui.auth.msal_helper import get_user_powerbi_token
from open_webui.utils.powerbi_schema import save_schema

router = APIRouter()

# --- Configuration ---
TENANT_ID = os.getenv("MICROSOFT_CLIENT_TENANT_ID")
CLIENT_ID = os.getenv("MICROSOFT_CLIENT_ID")
CLIENT_SECRET = os.getenv("MICROSOFT_CLIENT_SECRET")
CHARTING_URL = os.getenv("CHARTING_URL")
logger = logging.getLogger("webui_powerbi")

# Simple cache for workspaces with 10-minute expiration
workspace_cache = {
    "data": None,
    "timestamp": 0,
    "ttl": 600,  # 10 minutes
}

# ======================
# ASYNC HELPER FUNCTIONS
# ======================


async def http_get_powerbi_api(url: str, token: str, client: httpx.AsyncClient):
    """Reusable helper for GET requests to Power BI API."""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()  # Check for HTTP errors (like 401, 403, 404)
        return resp.json()
    except httpx.RequestError as e:
        logger.error(f"Power BI API request failed: {e.request.url} - {e}")
        raise HTTPException(status_code=502, detail=f"Power BI API Request Error: {e}")
    except httpx.HTTPStatusError as e:
        logger.error(
            f"Power BI API Error: {e.response.status_code} - {e.response.text}"
        )
        raise HTTPException(status_code=e.response.status_code, detail=e.response.text)


async def list_workspaces(token: str, client: httpx.AsyncClient):
    """Async: Fetches workspaces from Power BI API."""
    url = "https://api.powerbi.com/v1.0/myorg/groups"
    logger.info("Listing workspaces from Power BI API.")
    data = await http_get_powerbi_api(url, token, client)
    workspaces = data.get("value", [])
    logger.info(f"Found {len(workspaces)} workspaces.")
    return [{"name": ws["name"], "id": ws["id"]} for ws in workspaces]


async def list_datasets_in_workspace(
    token: str, workspace_id: str, client: httpx.AsyncClient
):
    """Async: Fetches datasets for a specific workspace."""
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    logger.info(f"Listing datasets in workspace: {workspace_id}")
    data = await http_get_powerbi_api(url, token, client)
    datasets = data.get("value", [])
    logger.info(f"Found {len(datasets)} datasets in workspace {workspace_id}.")
    return [{"name": ds["name"], "id": ds["id"]} for ds in datasets]


# ======================
# AUTH DEPENDENCY
# ======================


async def get_powerbi_token(request: Request) -> str:
    """
    FastAPI Dependency to get the Power BI token.
    It runs the *sync* get_user_powerbi_token function in a non-blocking thread.
    """
    try:
        # Run the sync/blocking token function in a thread pool
        token = await run_in_threadpool(get_user_powerbi_token, request)
        if not token:
            raise Exception("Token was empty")
        logger.info("✅ Got Power BI token from request.")
        return token
    except Exception as e:
        logger.error(f"❌ get_user_powerbi_token failed: {e}")
        raise HTTPException(
            status_code=401, detail=f"Token extraction or OBO flow failed: {str(e)}"
        )


# ======================
# API ENDPOINTS
# ======================


@router.get("/powerbi/workspaces")
async def list_workspaces_api(token: str = Depends(get_powerbi_token)):
    """Async: Lists all Power BI workspaces, with caching."""
    logger.info("🟡 /powerbi/workspaces called.")

    current_time = time.time()
    if (
        workspace_cache["data"] is not None
        and current_time - workspace_cache["timestamp"] < workspace_cache["ttl"]
    ):
        logger.info("Returning cached workspaces data.")
        return workspace_cache["data"]

    async with httpx.AsyncClient() as client:
        workspaces = await list_workspaces(token, client)

    workspace_cache["data"] = workspaces
    workspace_cache["timestamp"] = current_time
    logger.info(f"✅ Cached {len(workspaces)} workspaces for 10 minutes.")
    return workspaces


@router.get("/powerbi/workspaces/{workspace_id}/datasets")
async def list_datasets_api(workspace_id: str, token: str = Depends(get_powerbi_token)):
    """Async: Lists datasets in a specific Power BI workspace."""
    async with httpx.AsyncClient() as client:
        return await list_datasets_in_workspace(token, workspace_id, client)


@router.get("/powerbi/schema/{workspace_id}/{dataset_id}")
async def fetch_dataset_schema(
    workspace_id: str, dataset_id: str, token: str = Depends(get_powerbi_token)
):
    """Async: Fetches and caches the full schema for a dataset."""
    headers = {"Authorization": f"Bearer {token}"}
    base_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}"
    schema = {}

    async with httpx.AsyncClient() as client:
        # --- 1) FETCH TABLES ---
        try:
            logger.info(f"Fetching tables for dataset {dataset_id}")
            tables_data = await http_get_powerbi_api(
                f"{base_url}/tables", token, client
            )
        except HTTPException as e:
            logger.error(f"Failed to fetch tables: {e.detail}")
            return {"error": f"Failed to fetch tables: {e.detail}"}

        tables = tables_data.get("value", [])
        if not tables:
            return {"error": "No tables found in dataset."}

        logger.info(f"Found {len(tables)} tables. Fetching columns...")

        # --- 2) FETCH COLUMNS (in parallel) ---
        async def fetch_columns(table_name):
            try:
                cols_data = await http_get_powerbi_api(
                    f"{base_url}/tables/{table_name}/columns", token, client
                )
                return table_name, [col["name"] for col in cols_data.get("value", [])]
            except Exception as e:
                logger.warning(f"Could not fetch columns for table {table_name}: {e}")
                return table_name, []  # Return empty list on failure

        tasks = [fetch_columns(tbl["name"]) for tbl in tables]
        column_results = await asyncio.gather(*tasks)

        for table_name, columns in column_results:
            schema[table_name] = columns

    # --- 3) SAVE ---
    logger.info(f"Saving schema for dataset {dataset_id}")
    await run_in_threadpool(save_schema, dataset_id, schema)

    return {"dataset_id": dataset_id, "schema": schema, "tables_count": len(schema)}


@router.post("/powerbi/execute-query")
async def execute_query(request: Request):
    """
    🔥 NEW: Proxies a DAX query from the frontend tool call to the CHARTING_URL.
    This is the endpoint your frontend should call after the LLM requests a tool run.
    """
    if not CHARTING_URL:
        logger.error("❌ CHARTING_URL is not set. Cannot execute query.")
        raise HTTPException(
            status_code=500, detail="Charting service is not configured."
        )

    body = await request.json()
    logger.info(f"🟡 /powerbi/execute-query called. Proxying to CHARTING_URL.")

    # We assume the CHARTING_URL has an endpoint that can handle this
    # e.g., CHARTING_URL/execute-query
    charting_endpoint = f"{CHARTING_URL}/execute-query"

    async with httpx.AsyncClient() as client:
        try:
            charting_resp = await client.post(
                url=charting_endpoint,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                cookies=request.cookies,  # Forward the user's auth cookie
                json=body,  # Forward the body (dax_query, workspace_id, etc.)
                timeout=120.0,  # Give Power BI time to compute
            )
            charting_resp.raise_for_status()

            # Return the response (e.g., data or chart JSON) from the charting service
            return Response(
                content=charting_resp.content,
                status_code=charting_resp.status_code,
                headers=dict(charting_resp.headers),
            )
        except httpx.RequestError as e:
            logger.error(f"❌ Failed to proxy query to CHARTING_URL: {e}")
            raise HTTPException(
                status_code=502, detail=f"Charting service connection error: {e}"
            )
        except httpx.HTTPStatusError as e:
            logger.error(
                f"❌ Charting service returned error: {e.response.status_code} - {e.response.text}"
            )
            # Forward the error response from the charting service
            return Response(
                content=e.response.content,
                status_code=e.response.status_code,
                headers=dict(e.response.headers),
            )


@router.api_route("/powerbi/{path:path}", methods=["GET", "POST"])
async def proxy_to_charting(request: Request, path: str):
    """
    Generic fallback proxy to the charting backend, preserving cookies.
    Prefer using specific endpoints like /execute-query where possible.
    """
    if not CHARTING_URL:
        logger.error("❌ CHARTING_URL is not set. Cannot proxy.")
        raise HTTPException(
            status_code=500, detail="Charting service is not configured."
        )

    async with httpx.AsyncClient() as client:
        try:
            charting_resp = await client.request(
                method=request.method,
                url=f"{CHARTING_URL}/{path}",
                headers=request.headers,
                cookies=request.cookies,
                content=await request.body(),
                timeout=30.0,
            )
            # Return the response back to frontend
            return Response(
                content=charting_resp.content,
                status_code=charting_resp.status_code,
                headers=dict(charting_resp.headers),
            )
        except httpx.RequestError as e:
            logger.error(f"❌ Failed to proxy path '{path}' to CHARTING_URL: {e}")
            raise HTTPException(
                status_code=502, detail=f"Charting service proxy error: {e}"
            )
        except httpx.HTTPStatusError as e:
            logger.error(
                f"❌ Charting service returned error on path '{path}': {e.response.text}"
            )
            return Response(
                content=e.response.content,
                status_code=e.response.status_code,
                headers=dict(e.response.headers),
            )
