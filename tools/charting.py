def list_dataset_tables(dataset_id: str, token: str) -> list[str]:
    """
    Fetch all table names from a Power BI dataset (semantic or push).
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{POWERBI_WORKSPACE_ID}/datasets/{dataset_id}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json().get("value", [])
        tables = [t["name"] for t in data]
        logger.info(f"📘 Found {len(tables)} tables in dataset {dataset_id}: {tables}")
        return tables
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to list tables for dataset {dataset_id}: {e}")
        return []


# powerbi_viz_tool.py
import logging
import difflib
import re


# ======================
# COLUMN VALIDATION & CORRECTION
# ======================
def validate_and_correct_columns(dax_query: str, chosen_table: str) -> str:
    """
    Validates column names in the DAX query against COLUMN_CACHE for the chosen_table.
    If a column is not found, attempts to replace with a close match (>0.75 similarity).
    Logs a warning for replacements, and an error if no match is found (skipping execution).
    Returns the corrected DAX query string, or None if query should be skipped.
    """
    logger.info(f"🔍 Validating DAX query for table '{chosen_table}': {dax_query}")
    # Use regex to find all [COLUMN] references
    # Accepts [A-Z0-9_], handles spaces or special chars inside brackets
    columns_in_query = set(re.findall(r"(?:[A-Za-z0-9_]+)?\[(.*?)\]", dax_query))
    # Only validate if table is in cache
    valid_columns = set(COLUMN_CACHE.get(chosen_table, []))
    corrected_query = dax_query
    skip_query = False
    replacements_made = 0
    # Detect DAX aliases like "Total Profit" inside SUMMARIZE or ADDCOLUMNS
    alias_pattern = re.findall(r'"([^"]+)"\s*,\s*SUM\(', dax_query)
    alias_columns = set(alias_pattern)
    if alias_columns:
        logger.debug(
            f"🧠 Detected DAX aliases (excluded from validation): {alias_columns}"
        )
    for col in columns_in_query:
        if col not in valid_columns and col not in alias_columns:
            # Fuzzy match with lower cutoff and substring fallback
            matches = difflib.get_close_matches(col, valid_columns, n=1, cutoff=0.5)
            if not matches:
                for valid_col in valid_columns:
                    if (
                        col.lower() in valid_col.lower()
                        or valid_col.lower() in col.lower()
                    ):
                        matches = [valid_col]
                        break
            if matches:
                replacement = matches[0]
                # Replace [col] with [replacement] everywhere in query
                corrected_query = re.sub(
                    rf"\[{re.escape(col)}\]", f"[{replacement}]", corrected_query
                )
                logger.warning(
                    f"⚠️ Column '{col}' not found in {chosen_table}. Replaced with '{replacement}'"
                )
                replacements_made += 1
            else:
                logger.error(
                    f"❌ Column '{col}' not found in {chosen_table} and no close match found. Skipping query."
                )
                skip_query = True

    # Auto-correct invalid SUMMARIZE DAX pattern to ADDCOLUMNS
    summarize_pattern = re.compile(
        r"SUMMARIZE\s*\(([^,]+?),\s*([^,]+?),\s*\"([^\"]+)\",\s*SUM\(([^)]+)\)\s*\)",
        re.IGNORECASE,
    )
    if summarize_pattern.search(corrected_query):
        corrected_query = summarize_pattern.sub(
            r"ADDCOLUMNS(SUMMARIZE(\1, \2), \"\3\", SUM(\4))", corrected_query
        )
        logger.warning(
            "⚙️ Auto-corrected invalid DAX SUMMARIZE query to ADDCOLUMNS pattern."
        )

    # Ensure TOPN sort column matches alias defined in ADDCOLUMNS
    topn_sort_pattern = re.compile(
        r"TOPN\s*\(\s*\d+,\s*ADDCOLUMNS\(.*?\"([^\"]+)\".*?\),\s*\[([^\]]+)\]",
        re.IGNORECASE | re.DOTALL,
    )
    m = topn_sort_pattern.search(corrected_query)
    if m and m.group(1) != m.group(2):
        alias, sort_col = m.group(1), m.group(2)
        corrected_query = re.sub(
            rf"\[{re.escape(sort_col)}\]", f"[{alias}]", corrected_query
        )
        logger.warning(
            f"⚙️ Fixed TOPN sort expression: replaced [{sort_col}] with [{alias}]"
        )

    logger.info(
        f"✅ Validated {len(columns_in_query)} columns, {replacements_made} replacements made."
    )
    if skip_query:
        return None
    return corrected_query


import os
import json
import pandas as pd
import requests
import html
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from msal import ConfidentialClientApplication
import plotly.express as px
from time import time, sleep
from typing import Optional
from functools import wraps
from pathlib import Path

# ======================
# ENV & LOGGING
load_dotenv()


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("powerbi_backend")


# Simple in-memory cache for schema discovery (dataset_id -> (timestamp, schema))
SCHEMA_CACHE: dict[str, tuple[float, dict]] = {}
SCHEMA_CACHE_TTL = int(os.getenv("SCHEMA_CACHE_TTL", "3600"))  # seconds

# ==========================================
# Column Cache (Memory + Persistent JSON)
# ==========================================
COLUMN_CACHE_FILE = Path(__file__).parent / "cache" / "column_cache.json"
COLUMN_CACHE_FILE.parent.mkdir(exist_ok=True)
COLUMN_CACHE: dict[str, list[str]] = {}
# Load cache from disk if available
if COLUMN_CACHE_FILE.exists():
    try:
        with open(COLUMN_CACHE_FILE, "r") as f:
            COLUMN_CACHE = json.load(f)
        logger.info(
            f"📂 Loaded column cache from {COLUMN_CACHE_FILE} ({len(COLUMN_CACHE)} tables)."
        )
    except Exception as e:
        logger.warning(f"⚠️ Failed to load column cache file: {e}")
# ======================
# CONFIG
# ======================
# ======================
# CONFIG
# ======================
MAX_PBI_RETRIES = int(os.getenv("MAX_PBI_RETRIES", "2"))
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
POWERBI_WORKSPACE_ID = os.getenv("POWERBI_WORKSPACE_ID")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
GPT_MODEL = "gpt-4o"


# Power BI Scope for On-Behalf-Of flow
POWERBI_SCOPE = os.getenv("POWERBI_SCOPE")

# For backend URL routing (OpenWebUI backend)
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8080")

# --- Startup Check ---
required_vars = {
    "TENANT_ID": TENANT_ID,
    "CLIENT_ID": CLIENT_ID,
    "CLIENT_SECRET": CLIENT_SECRET,
    "POWERBI_WORKSPACE_ID": POWERBI_WORKSPACE_ID,
    "AZURE_OPENAI_API_KEY": AZURE_OPENAI_API_KEY,
    "AZURE_OPENAI_ENDPOINT": AZURE_OPENAI_ENDPOINT,
}
missing = [k for k, v in required_vars.items() if not v]
if missing:
    msg = f"❌ Missing required env vars: {', '.join(missing)}"
    logger.critical(msg)
    raise SystemExit(msg)
logger.info("✅ All required environment variables are present.")


# ======================
# UNIVERSAL DATASET TYPE & PUSH DATASET FALLBACK HELPERS
# ======================


def get_dataset_type(dataset_info: dict) -> str:
    """
    Determines if the dataset is 'semantic' or 'push' based on metadata.
    """
    name = dataset_info.get("name", "unknown")
    if dataset_info.get("addRowsAPIEnabled"):
        logger.info(
            f"📤 Dataset '{name}' detected as Push dataset (addRowsAPIEnabled=True)."
        )
        return "push"

    if (
        dataset_info.get("isOnPremGatewayRequired")
        or dataset_info.get("targetStorageMode") == "Abf"
    ):
        logger.info(f"🧠 Dataset '{name}' detected as Semantic dataset.")
        return "semantic"

    # For modern semantic models that may not expose both flags
    if not dataset_info.get("addRowsAPIEnabled", False):
        logger.info(f"🧠 Dataset '{name}' inferred as Semantic dataset (no push flag).")
        return "semantic"

    logger.warning(
        f"⚠️ Could not determine dataset type for '{name}', defaulting to semantic."
    )
    return "semantic"


def list_push_dataset_tables(dataset_id: str, token: str) -> list[str]:
    """
    Lists tables for a push dataset.
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{POWERBI_WORKSPACE_ID}/datasets/{dataset_id}/tables"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        tables = resp.json().get("value", [])
        return [tbl["name"] for tbl in tables]
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to list tables for push dataset {dataset_id}: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to list push dataset tables."
        )


def get_push_dataset_rows(
    dataset_id: str, table_name: str, token: str, limit: int = 50
) -> list[dict]:
    """
    Fetches up to `limit` rows from a push dataset table.
    """
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{POWERBI_WORKSPACE_ID}/datasets/{dataset_id}/tables/{table_name}/rows"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        rows = resp.json().get("value", [])
        return rows[:limit]
    except requests.exceptions.RequestException as e:
        logger.error(
            f"Failed to get rows for push dataset {dataset_id}, table {table_name}: {e}"
        )
        raise HTTPException(status_code=500, detail="Failed to get push dataset rows.")


# ======================
# FASTAPI APP
# ======================
app = FastAPI(title="Power BI Visualization Service")


# Custom OpenAPI to match OpenWebUI tool format
def custom_openapi():
    return {
        "openapi": "3.1.0",
        "info": {"title": "Power BI Charting Tool", "version": "1.0.0"},
        "paths": {
            "/auth/tool/powerbi": {
                "post": {
                    "summary": "Process Power BI Requests",
                    "description": "Use this tool for any Power BI data/chart queries. It supports Teams SSO and standard JWT-based login.",
                    "operationId": "powerbi_tool_handler",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"prompt": {"type": "string"}},
                                    "required": ["prompt"],
                                }
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Successful Response",
                            "content": {"text/html": {}},
                        }
                    },
                }
            }
        },
    }


app.openapi = custom_openapi
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger.info("🚀 FastAPI app initialized with CORS and custom OpenAPI.")


# ======================
# BASE MODEL
# ======================
class PromptRequest(BaseModel):
    prompt: str


# ======================
# HELPER FUNCTIONS
# ======================
def get_obo_token(user_token: str) -> str:
    if not user_token or len(user_token) < 50:
        logger.warning(
            "No valid user token detected, using client_credentials fallback."
        )
        authority = f"https://login.microsoftonline.com/{TENANT_ID}"
        app = ConfidentialClientApplication(
            CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
        )
        result = app.acquire_token_for_client(scopes=[POWERBI_SCOPE])
        if "access_token" not in result:
            raise HTTPException(status_code=401, detail="Failed to get app token.")
        return result["access_token"]
    logger.info("Attempting On-Behalf-Of token acquisition.")
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(
        client_id=CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )

    # Define the specific scopes needed for OBO with Power BI
    specific_scopes = [
        "https://analysis.windows.net/powerbi/api/Dataset.Read.All",
        "https://analysis.windows.net/powerbi/api/Workspace.Read.All",
        "offline_access",
    ]
    logger.debug(f"Requesting OBO token with scopes: {specific_scopes}")

    # Truncate token for logging security
    user_token_display = (
        f"{user_token[:10]}...{user_token[-4:]}" if len(user_token) > 14 else user_token
    )
    logger.debug(f"Using user assertion (token starting with): {user_token_display}")

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


def list_datasets_in_workspace(token: str, workspace_id: str | None = None):
    workspace_id = workspace_id or POWERBI_WORKSPACE_ID
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    headers = {"Authorization": f"Bearer {token}"}
    logger.info(f"Listing datasets in workspace: {workspace_id}")
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        datasets = resp.json().get("value", [])
        logger.info(f"Found {len(datasets)} datasets in {workspace_id}.")
        return [{"name": ds["name"], "id": ds["id"]} for ds in datasets]
    except requests.exceptions.RequestException as e:
        logger.error(f"Power BI API request failed for listing datasets: {e}")
        status_code = (
            e.response.status_code if hasattr(e, "response") and e.response else 500
        )
        raise HTTPException(status_code=status_code, detail=str(e)) from e


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


def _call_powerbi_api(
    method: str,
    url: str,
    token: str,
    json_data: Optional[dict] = None,
    max_retries: int = MAX_PBI_RETRIES,
):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            if method.upper() == "GET":
                resp = requests.get(url, headers=headers, timeout=30)
            else:
                resp = requests.post(url, headers=headers, json=json_data, timeout=30)
            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError:
                return {"raw_text": resp.text}
        except requests.exceptions.RequestException as e:
            last_exc = e
            logger.warning(
                f"Power BI API call failed (attempt {attempt}/{max_retries}): {e}"
            )
            sleep(0.5 * attempt)
            continue
    status_code = (
        getattr(last_exc.response, "status_code", 500)
        if last_exc is not None and hasattr(last_exc, "response")
        else 500
    )
    raise HTTPException(
        status_code=status_code,
        detail=f"Power BI API Error after {max_retries} attempts: {last_exc}",
    )


def query_dataset(dax_query: str, dataset_id: str, token: str):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{POWERBI_WORKSPACE_ID}/datasets/{dataset_id}/executeQueries"
    body = {"queries": [{"query": dax_query}]}
    logger.info(f"Executing DAX query on dataset: {dataset_id}")
    logger.debug(f"DAX Query: {dax_query}")
    max_retries = 3
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            return _call_powerbi_api("POST", url, token, json_data=body)
        except Exception as e:
            last_exc = e
            if attempt == max_retries:
                logger.error(
                    f"❌ DAX query failed after {max_retries} attempts: {last_exc}"
                )
                raise
            sleep(0.5 * attempt)


def get_semantic_model_schema(dataset_id: str, token: str) -> dict:
    now = time()
    cached = SCHEMA_CACHE.get(dataset_id)
    if cached and (now - cached[0]) < SCHEMA_CACHE_TTL:
        logger.info(f"Using cached schema for dataset {dataset_id}")
        return cached[1]

    logger.info(f"🔍 Discovering schema for dataset: {dataset_id}")
    schema: dict = {}
    base_url = f"https://api.powerbi.com/v1.0/myorg/groups/{POWERBI_WORKSPACE_ID}/datasets/{dataset_id}/executeQueries"

    # Try TMSCHEMA DMVs first (best for semantic models)
    tables_body = {
        "queries": [
            {
                "query": (
                    "EVALUATE SELECTCOLUMNS("
                    "$SYSTEM.TMSCHEMA_TABLES,"
                    '"TableName", [Name],'
                    '"TableID", [ID])'
                )
            }
        ]
    }

    try:
        resp = _call_powerbi_api("POST", base_url, token, json_data=tables_body)
        rows = resp.get("results", [])[0].get("tables", [])[0].get("rows", [])
        if not rows:
            raise HTTPException(
                status_code=404, detail="No tables found in semantic model."
            )

        for row in rows:
            table_name = row.get("TableName")
            table_id = row.get("TableID")
            col_body = {
                "queries": [
                    {
                        "query": (
                            f"EVALUATE SELECTCOLUMNS(FILTER($SYSTEM.TMSCHEMA_COLUMNS, [TableID]={table_id}),"
                            '"ColumnName", [Name],"DataType", [DataType])'
                        )
                    }
                ]
            }

            col_resp = _call_powerbi_api("POST", base_url, token, json_data=col_body)
            col_rows = (
                col_resp.get("results", [])[0].get("tables", [])[0].get("rows", [])
            )
            schema[table_name] = [
                c.get("ColumnName") for c in col_rows if c.get("ColumnName")
            ]

        SCHEMA_CACHE[dataset_id] = (now, schema)
        logger.info(
            f"✅ Retrieved schema with {len(schema)} tables from dataset {dataset_id}"
        )
        return schema

    except HTTPException as e:
        status = getattr(e, "status_code", 500)
        if status in (400, 403):
            logger.warning(
                f"DMV schema discovery blocked (status {status}). Falling back to DAX-based schema discovery."
            )
            try:
                schema = get_dataset_schema_via_dax(dataset_id, token)
                SCHEMA_CACHE[dataset_id] = (now, schema)
                return schema
            except Exception as dax_fallback_error:
                logger.error(f"DAX fallback failed: {dax_fallback_error}")
                logger.warning("⚙️ Attempting final EVALUATE-based fallback.")
                schema = get_schema_via_evaluate(dataset_id, token)
                SCHEMA_CACHE[dataset_id] = (now, schema)
                return schema
        else:
            logger.error(f"Schema discovery failed: {e}")
            raise
    except Exception as e:
        logger.error(f"Schema discovery unexpected error: {e}")
        try:
            schema = get_schema_via_evaluate(dataset_id, token)
            SCHEMA_CACHE[dataset_id] = (now, schema)
            return schema
        except Exception as eval_error:
            logger.exception(f"EVALUATE fallback failed: {eval_error}")
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve semantic model schema (all fallbacks failed).",
            )


# ======================
# EVALUATE-BASED ULTIMATE FALLBACK SCHEMA DISCOVERY
# ======================
def get_schema_via_evaluate(dataset_id: str, token: str) -> dict:
    logger.info(f"Running EVALUATE-based schema discovery for dataset: {dataset_id}")
    schema = {}
    candidate_tables = list_dataset_tables(dataset_id, token)
    if not candidate_tables:
        logger.warning("⚠️ No tables found via API, falling back to manual candidates.")
        candidates_env = os.getenv("POWERBI_EVALUATE_CANDIDATES")
        candidate_tables = (
            [t.strip() for t in candidates_env.split(",")]
            if candidates_env
            else ["Sheet1"]
        )

    for table in candidate_tables:
        if table in COLUMN_CACHE:
            logger.info(f"🗃️ Skipping already cached table '{table}'.")
            continue
        dax_query = f"EVALUATE TOPN(1, '{table}')"
        try:
            result = query_dataset(dax_query, dataset_id, token)
            results_list = result.get("results", [])
            if not results_list:
                continue
            tables = results_list[0].get("tables", [])
            if not tables or not tables[0].get("rows"):
                continue
            sample_row = tables[0]["rows"][0]
            cols = [key.split("[", 1)[1].rstrip("]") for key in sample_row.keys()]
            if cols:
                schema[table] = cols
                logger.info(f"✅ Found table '{table}' with {len(cols)} columns.")
                if table not in COLUMN_CACHE:
                    COLUMN_CACHE[table] = cols
                    save_column_cache()
                    logger.info(
                        f"💾 Added new table '{table}' with {len(cols)} columns to local cache."
                    )
                else:
                    logger.debug(
                        f"🗃️ Table '{table}' already present in cache; skipping save."
                    )
                break  # ✅ Stop after first successful schema
        except Exception as e:
            logger.warning(f"Skipping table '{table}' (not found or inaccessible): {e}")
            continue
        finally:
            sleep(0.1)

    if not schema:
        raise HTTPException(
            status_code=404,
            detail="No tables could be discovered via EVALUATE fallback.",
        )
    return schema


def build_schema_context(schema: dict) -> list[str]:
    context = []
    for tbl, cols in schema.items():
        if cols:
            formatted_cols = ", ".join(cols[:20])  # up to 20 columns for clarity
        else:
            formatted_cols = "No columns"
        context.append(f"Table: {tbl} | Columns: {formatted_cols}")
    return context


def save_column_cache():
    """Write current COLUMN_CACHE to JSON file."""
    try:
        with open(COLUMN_CACHE_FILE, "w") as f:
            json.dump(COLUMN_CACHE, f, indent=2)
        logger.info(f"💾 Column cache saved to {COLUMN_CACHE_FILE}")
    except Exception as e:
        logger.error(f"❌ Failed to save column cache: {e}")


def get_columns_for_table(chosen_table: str, dataset_id: str, token: str) -> list[str]:
    """Get columns for a given table (from cache or live query)."""
    # 1️⃣ Try cache first
    if chosen_table in COLUMN_CACHE:
        logger.info(f"🗃️ Using cached columns for table '{chosen_table}'")
        return COLUMN_CACHE[chosen_table]

    # 2️⃣ Otherwise infer dynamically via Power BI query
    logger.info(f"🔍 Fetching columns for table '{chosen_table}' via TOPN(1)")
    try:
        dax = f"EVALUATE TOPN(1, '{chosen_table}')"
        result = query_dataset(dax, dataset_id, token)
        results_list = result.get("results", [])
        if results_list:
            tables = results_list[0].get("tables", [])
            if tables and tables[0].get("rows"):
                sample_row = tables[0]["rows"][0]
                columns = [
                    key.split("[", 1)[1].rstrip("]") for key in sample_row.keys()
                ]
                COLUMN_CACHE[chosen_table] = columns
                save_column_cache()  # persist dynamically
                logger.info(
                    f"✅ Added {len(columns)} columns for table '{chosen_table}' to cache."
                )
                return columns
    except Exception as e:
        logger.warning(f"⚠️ Failed to fetch columns for '{chosen_table}': {e}")
    return []


# ======================
# GET TABLE COLUMNS
# ======================
def get_table_columns(table_name: str, dataset_id: str, token: str):
    dax_query = f"EVALUATE TOPN(1, '{table_name}')"
    try:
        result = query_dataset(dax_query, dataset_id, token)

        results_list = result.get("results", [])
        if not results_list:
            logger.warning("Power BI query returned no 'results' list.")
            return []

        tables = results_list[0].get("tables", [])
        if not tables or not tables[0].get("rows"):
            logger.warning("Power BI query returned no tables or no rows.")
            return []

        sample_row = tables[0]["rows"][0]
        # Clean column names like 'Sheet1[CUSTOMER_NAME]' to 'CUSTOMER_NAME'
        cols = [key.split("[", 1)[1].rstrip("]") for key in sample_row.keys()]
        return cols
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.exception(f"Unexpected error getting columns for table: {table_name}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error getting columns for table '{table_name}'.",
        ) from e


def get_dataset_schema_via_dax(dataset_id: str, token: str) -> dict:
    """Retrieves tables and columns using DAX DMV queries."""
    schema = {}
    logger.info(f"Getting dataset schema via DAX for dataset: {dataset_id}")
    try:
        # Query 1: Get Table Names using MDSCHEMA_TABLES
        tables_query = (
            "SELECT TABLE_NAME FROM $SYSTEM.MDSCHEMA_TABLES WHERE TABLE_TYPE = 'TABLE'"
        )
        logger.debug("Executing DAX query to get table names.")
        tables_result = query_dataset(tables_query, dataset_id, token)

        tables_rows = (
            tables_result.get("results", [])[0].get("tables", [])[0].get("rows", [])
        )
        if not tables_rows:
            logger.warning(
                f"MDSCHEMA_TABLES query returned no tables for dataset {dataset_id}"
            )
            return {}

        table_names = [row["[TABLE_NAME]"] for row in tables_rows]
        logger.info(f"Found potential tables via DAX: {table_names}")

        # Query 2: Get Columns for each table using MDSCHEMA_COLUMNS
        # Note: This makes multiple API calls, one per table.
        for table_name in table_names:
            # Skip internal/hidden tables if necessary (often start with $)
            if (
                table_name.startswith("$")
                or table_name.startswith("LocalDateTable_")
                or table_name.startswith("DateTableTemplate_")
            ):
                logger.debug(f"Skipping potentially internal table: {table_name}")
                continue

            columns_query = f"SELECT COLUMN_NAME FROM $SYSTEM.MDSCHEMA_COLUMNS WHERE TABLE_NAME = '{table_name}'"
            logger.debug(f"Executing DAX query to get columns for table: {table_name}")
            try:
                cols_result = query_dataset(columns_query, dataset_id, token)
                cols_rows = (
                    cols_result.get("results", [])[0]
                    .get("tables", [])[0]
                    .get("rows", [])
                )
                column_names = [row["[COLUMN_NAME]"] for row in cols_rows]
                if column_names:
                    schema[table_name] = column_names
                    logger.debug(f"Found columns for '{table_name}': {column_names}")
                else:
                    logger.warning(
                        f"MDSCHEMA_COLUMNS query returned no columns for table '{table_name}'."
                    )
            except Exception as col_err:
                # Log error for specific table but continue trying others
                logger.error(
                    f"Failed to get columns for table '{table_name}': {col_err}"
                )

        if not schema:
            logger.error(
                f"Could not retrieve valid schema for any tables in dataset {dataset_id} using DAX DMVs."
            )
            # Raise error if NO tables could be processed
            raise HTTPException(
                status_code=404,
                detail=f"Could not retrieve schema information for dataset '{dataset_id}'.",
            )

        logger.info(
            f"Successfully retrieved schema for {len(schema)} tables using DAX."
        )
        return schema

    except HTTPException as http_ex:
        # Re-raise exceptions from query_dataset (like 401, 403)
        logger.error(f"HTTPException while getting schema via DAX: {http_ex.detail}")
        raise http_ex
    except Exception as e:
        logger.exception(
            f"Unexpected error getting schema via DAX for dataset: {dataset_id}"
        )
        raise HTTPException(
            status_code=500, detail="Internal error getting dataset schema via DAX."
        ) from e

    except requests.exceptions.RequestException as e:
        logger.error(f"Power BI REST API request failed while getting schema: {e}")
        status_code = (
            e.response.status_code
            if hasattr(e, "response") and e.response is not None
            else 500
        )
        detail = f"Power BI API Error getting schema: {str(e)}"
        raise HTTPException(status_code=status_code, detail=detail) from e
    except Exception as e:
        logger.exception(
            f"Unexpected error parsing schema from REST API for dataset: {dataset_id}"
        )
        raise HTTPException(
            status_code=500, detail="Internal error parsing dataset schema."
        ) from e


def fetch_chart_data(
    dax_queries: list,
    dataset_id: str,
    token: str,
    dataset_info: dict = None,
    chosen_table: str = None,
):
    logger.info(
        f"Fetching chart data from dataset {dataset_id} using {len(dax_queries)} queries."
    )
    all_dfs = []
    is_push = False
    if dataset_info:
        dtype = get_dataset_type(dataset_info)
        is_push = dtype == "push"
    if not chosen_table:
        # Try to guess from first DAX query if not provided
        # Look for FROM or TOPN(, '<table>')
        table_guess = None
        for q in dax_queries:
            m = re.search(r"(?:FROM|TOPN\([^,]+,\s*'([^']+)')", q, re.IGNORECASE)
            if m:
                table_guess = m.group(1)
                break
        chosen_table = table_guess
    try:
        for i, q in enumerate(dax_queries):
            # Validate and correct columns before executing
            table_for_query = chosen_table
            # Optionally: try to extract table for each query if needed
            # (For now, use chosen_table for all queries)
            if table_for_query:
                corrected_q = validate_and_correct_columns(q, table_for_query)
                if corrected_q is None:
                    logger.error(f"Skipping DAX query {i + 1} due to column errors.")
                    continue  # Skip this query
            else:
                corrected_q = q  # No validation if table not known
            logger.info(
                f"🧩 Executing DAX Query for dataset {dataset_id}: {corrected_q}"
            )
            logger.debug(f"Executing query {i + 1}/{len(dax_queries)}: {corrected_q}")
            try:
                result = query_dataset(
                    corrected_q, dataset_id, token
                )  # query_dataset logs errors
                logger.info(
                    f"✅ DAX Query executed successfully for dataset {dataset_id}"
                )
                tables = result.get("results", [])[0].get("tables", [])
                if tables and tables[0].get("rows"):
                    all_dfs.append(pd.DataFrame(tables[0]["rows"]))
                else:
                    logger.warning(f"Query {i + 1} returned no tables: {corrected_q}")
            except HTTPException as dex:
                logger.warning(f"DAX query failed: {dex.detail}")
                if is_push:
                    logger.warning("🔁 Falling back to Push Dataset mode (DAX failed).")
                    match = re.search(
                        r"(?:FROM|TOPN\([^,]+,\s*'([^']+)')", q, re.IGNORECASE
                    )
                    table_name = None
                    if match:
                        table_name = match.group(1)
                    try:
                        tables_list = list_push_dataset_tables(dataset_id, token)
                        if not table_name and tables_list:
                            table_name = tables_list[0]
                        if table_name:
                            rows = get_push_dataset_rows(dataset_id, table_name, token)
                            if rows:
                                all_dfs.append(pd.DataFrame(rows))
                                logger.info("Push dataset fallback data loaded.")
                            else:
                                logger.warning(
                                    "Push dataset fallback: No rows returned."
                                )
                        else:
                            logger.error("Push dataset fallback: No table name found.")
                    except Exception as fallback_ex:
                        logger.error(f"Push dataset fallback failed: {fallback_ex}")
                else:
                    raise
        if not all_dfs:
            logger.warning(
                "No data returned from Power BI for any DAX query. Checking for Push fallback..."
            )
            if is_push:
                logger.warning(
                    "🔁 Falling back to Push Dataset mode (DAX returned empty)."
                )
                try:
                    tables_list = list_push_dataset_tables(dataset_id, token)
                    if tables_list:
                        rows = get_push_dataset_rows(dataset_id, tables_list[0], token)
                        if rows:
                            all_dfs.append(pd.DataFrame(rows))
                except Exception as fallback_ex:
                    logger.error(f"Push dataset fallback failed: {fallback_ex}")
            if not all_dfs:
                logger.error(
                    "No data returned from Power BI for any DAX query or Push fallback."
                )
                raise HTTPException(
                    status_code=404,
                    detail="No data returned from Power BI for the generated queries.",
                )

        logger.info(f"Successfully fetched data, merging {len(all_dfs)} dataframes.")
        df_final = all_dfs[0]
        if len(all_dfs) > 1:
            x_col = df_final.columns[0]  # Assuming first column is the merge key
            logger.debug(f"Merging multiple dataframes on column: {x_col}")
            for df_next in all_dfs[1:]:
                df_final = pd.merge(df_final, df_next, on=x_col, how="outer")

        logger.info(f"📊 Final merged DataFrame shape: {df_final.shape}")
        logger.info("Data fetching and merging complete.")
        return df_final

    except HTTPException as http_ex:
        # Re-raise HTTP exceptions from query_dataset
        raise http_ex
    except Exception as e:
        logger.exception(
            f"Error during chart data fetching or merging for dataset {dataset_id}."
        )
        raise HTTPException(
            status_code=500, detail="Internal error fetching or processing chart data."
        ) from e


def choose_relevant_dataset(prompt: str, dataset_names: list[str]) -> str:
    logger.info("Asking LLM to choose relevant dataset.")
    logger.debug(f"Prompt: '{prompt}', Available Datasets: {dataset_names}")
    headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}
    system_content = "From the list, return ONLY the single most relevant dataset name for the user's prompt."
    user_content = f"Prompt: '{prompt}'\n\nDatasets: {json.dumps(dataset_names)}\n\nMost relevant dataset name?"
    body = {
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0,
    }
    api_version = "2024-02-01"
    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{GPT_MODEL}/chat/completions?api-version={api_version}"
    try:
        resp = requests.post(url, headers=headers, json=body)
        resp.raise_for_status()
        chosen_dataset = (
            resp.json()["choices"][0]["message"]["content"].strip().strip('"')
        )
        logger.info(f"LLM chose dataset: '{chosen_dataset}'")
        if chosen_dataset not in dataset_names:
            logger.warning(
                f"LLM chose invalid dataset '{chosen_dataset}'. Falling back to first."
            )
            if dataset_names:
                return dataset_names[0]
            else:
                raise HTTPException(
                    status_code=500, detail="LLM failed: No valid datasets available."
                )
        return chosen_dataset
    except requests.exceptions.RequestException as e:
        logger.error(f"Azure OpenAI API request failed: {e}")
        status_code = (
            e.response.status_code
            if hasattr(e, "response") and e.response is not None
            else 500
        )
        raise HTTPException(
            status_code=status_code, detail=f"Azure OpenAI Error: {str(e)}"
        ) from e
    except Exception as e:
        logger.exception("Unexpected error choosing dataset.")
        raise HTTPException(
            status_code=500, detail="Internal error choosing dataset."
        ) from e


def guess_relevant_table(
    prompt: str, dataset_name: str, dataset_id: str, token: str
) -> str:
    """Use semantic model schema instead of fallback tables."""
    logger.info(
        f"🔍 Inferring relevant table for '{prompt}' in dataset '{dataset_name}'"
    )
    schema = get_semantic_model_schema(dataset_id, token)
    if not schema:
        raise HTTPException(
            status_code=404, detail=f"No schema found for dataset '{dataset_name}'."
        )

    schema_context = build_schema_context(schema)
    chosen_table = choose_best_match(prompt, schema_context, "table")

    # Extract clean table name from "Sales (Customer, Amount)" style string
    clean_table = chosen_table.split(" (")[0].strip()
    logger.info(f"✅ Chosen table: {clean_table}")
    return clean_table


def get_llm_plan(prompt, columns, table_name):
    logger.info("Asking LLM to determine intent and generate DAX plan.")
    headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}
    context_hint = ""
    if table_name:
        context_hint += f"Table: {table_name}. "
    if columns:
        sample_cols = ", ".join(columns[:10])
        context_hint += f"Columns: {sample_cols}. "

    system_content = (
        "You are an assistant for a Power BI integration tool. "
        "You must always return a JSON object with an 'intent' field. "
        "Possible intents are: 'list_workspaces', 'list_datasets', 'list_tables', and 'generate_chart'. "
        "If 'generate_chart', include 'chart_type' and 'dax_queries' (a list of EVALUATE DAX strings)."
    )

    body = {
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Context: {context_hint}\nUser: {prompt}"},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    api_version = "2024-02-01"
    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{GPT_MODEL}/chat/completions?api-version={api_version}"
    try:
        resp = requests.post(url, headers=headers, json=body)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        plan = json.loads(content)
        logger.info(f"LLM generated plan with intent: '{plan.get('intent')}'")
        return plan
    except requests.exceptions.RequestException as e:
        logger.error(f"Azure OpenAI API request failed for getting plan: {e}")
        status_code = (
            e.response.status_code
            if hasattr(e, "response") and e.response is not None
            else 500
        )
        raise HTTPException(
            status_code=status_code, detail=f"Azure OpenAI Error: {str(e)}"
        ) from e
    except json.JSONDecodeError as e:
        raw = (
            resp.json()["choices"][0]["message"]["content"] if resp is not None else ""
        )
        logger.error(f"Failed JSON decode for plan: {e}. Raw: {raw}")
        raise HTTPException(
            status_code=500, detail="Internal error: LLM plan invalid JSON."
        ) from e


# ======================
# API ENDPOINTS
# ======================
class ToolRequest(BaseModel):
    prompt: str
    workspace_id: str | None = None
    dataset_id: str | None = None


@app.get("/powerbi/workspaces")
def get_workspaces(request: Request):
    cookie_token = request.cookies.get("access_token") or request.cookies.get(
        "teams_token"
    )
    if not cookie_token:
        raise HTTPException(status_code=401, detail="No user token provided")
    token = get_obo_token(cookie_token)
    return list_workspaces(token)


@app.get("/powerbi/workspaces/{workspace_id}/datasets")
def get_datasets(workspace_id: str, request: Request):
    cookie_token = request.cookies.get("access_token") or request.cookies.get(
        "teams_token"
    )
    if not cookie_token:
        raise HTTPException(status_code=401, detail="No user token provided")
    token = get_obo_token(cookie_token)
    # Modify list_datasets_in_workspace to accept workspace_id
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("value", [])


@app.post("/auth/tool/powerbi")
def handle_tool_call(req: ToolRequest, http_request: Request):
    logger.info(f"Received tool call for prompt: '{req.prompt}'")
    # Try from Authorization header, cookie, or session (for browser users)

    auth_header = http_request.headers.get("Authorization")
    cookie_token = http_request.cookies.get("access_token") or http_request.cookies.get(
        "teams_token"
    )

    if auth_header and auth_header.startswith("Bearer "):
        user_token = auth_header.split(" ")[1]
    elif cookie_token:
        user_token = cookie_token
    else:
        logger.error("No authorization token found in headers or cookies.")
        raise HTTPException(status_code=401, detail="Authentication token missing.")

    try:
        logger.debug("Attempting OBO token exchange...")
        powerbi_token = get_obo_token(
            user_token
        )  # OBO function logs errors internally now
        logger.info("OBO token obtained, proceeding to process prompt.")
        return process_prompt(req, powerbi_token, req.workspace_id, req.dataset_id)
    # Catch specific OBO failure
    except HTTPException as http_ex:
        if http_ex.status_code == 401:
            logger.error(
                f"Tool call failed due to OBO token acquisition failure: {http_ex.detail}"
            )
            # Re-raise with potentially cleaner detail for the caller (OpenWebUI)
            raise HTTPException(
                status_code=500, detail=f"Authentication error: {http_ex.detail}"
            ) from http_ex
        else:
            logger.error(f"HTTP Exception during tool call: {http_ex.detail}")
            raise http_ex  # Re-raise other HTTP exceptions
    except Exception as e:
        # Catch any other unexpected errors during OBO or process_prompt call
        logger.exception("Unexpected error during tool call orchestration.")
        detail = getattr(e, "detail", str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error during tool execution: {detail}",
        ) from e


def choose_best_match(prompt: str, candidates: list[str], label: str) -> str:
    logger.info(
        f"Asking LLM to select best {label} for: '{prompt}' from {len(candidates)} candidates"
    )
    headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}
    system_msg = (
        f"You are a Power BI expert assistant. The user asked: '{prompt}'."
        f" From the following list of {label}s, choose the one that most likely contains relevant data."
        f" Respond ONLY with the exact name from the list — no extra text, punctuation, or explanation."
    )
    body = {
        "messages": [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": f"Available {label}s: {candidates}"},
        ],
        "temperature": 0,
    }
    api_version = "2024-02-01"
    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{GPT_MODEL}/chat/completions?api-version={api_version}"

    try:
        resp = requests.post(url, headers=headers, json=body)
        resp.raise_for_status()
        choice_raw = resp.json()["choices"][0]["message"]["content"].strip()
        choice = choice_raw.strip('"').strip("'")

        def normalize_name(x: str) -> str:
            if "Table:" in x:
                return x.split("Table:")[1].split("|")[0].strip()
            if "|" in x:
                return x.split("|")[0].strip()
            return x.strip()

        normalized_candidates = [normalize_name(c) for c in candidates]
        normalized_choice = normalize_name(choice)

        if normalized_choice not in normalized_candidates:
            logger.warning(
                f"LLM chose invalid {label} '{choice}', defaulting to first candidate."
            )
            return normalized_candidates[0]
        logger.info(f"✅ Selected {label}: {normalized_choice}")
        return normalized_choice
    except Exception as e:
        logger.error(f"LLM failed to choose {label}: {e}")
        return candidates[0] if candidates else None


def process_prompt(
    req: PromptRequest,
    powerbi_token: str,
    workspace_id: str | None = None,
    dataset_id: str | None = None,
):
    logger.info(f"Processing prompt: '{req.prompt}'")

    try:
        llm_plan = get_llm_plan(req.prompt, [], "")
        intent = llm_plan.get("intent")
        if not intent:
            intent = "list_datasets"
            logger.warning("⚠️ No intent detected — defaulting to 'list_datasets'")

        logger.info(f"Determined intent: {intent}")

        # ----------- Simple intents -----------
        if intent == "list_workspaces":
            workspaces = list_workspaces(powerbi_token)
            items_html = "".join(f"<li>{html.escape(name)}</li>" for name in workspaces)
            return HTMLResponse(
                f"<html><body style='background:#111;color:#fff;'><h3>Available Workspaces:</h3><ul>{items_html}</ul></body></html>"
            )

        elif intent == "list_datasets":
            datasets = list_datasets_in_workspace(powerbi_token, workspace_id)
            dataset_names = [ds["name"] for ds in datasets]
            items_html = "".join(
                f"<li>{html.escape(name)}</li>" for name in dataset_names
            )
            return HTMLResponse(
                f"<html><body style='background:#111;color:#fff;'><h3>Available Datasets:</h3><ul>{items_html}</ul></body></html>"
            )

        elif intent == "list_tables":
            datasets = list_datasets_in_workspace(powerbi_token, workspace_id)
            dataset_names = [ds["name"] for ds in datasets]
            chosen_dataset = choose_best_match(req.prompt, dataset_names, "dataset")
            selected_dataset = next(
                (d for d in datasets if d["name"] == chosen_dataset), None
            )

            if not selected_dataset:
                raise HTTPException(
                    status_code=404, detail=f"Dataset '{chosen_dataset}' not found."
                )

            dtype = get_dataset_type(selected_dataset)
            if dtype == "semantic":
                schema = get_semantic_model_schema(
                    selected_dataset["id"], powerbi_token
                )
            elif dtype == "push":
                schema = {
                    tbl: []
                    for tbl in list_push_dataset_tables(
                        selected_dataset["id"], powerbi_token
                    )
                }
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Unknown dataset type or unsupported schema query.",
                )

            items_html = "".join(
                f"<li>{html.escape(tbl)} ({len(cols)} columns)</li>"
                for tbl, cols in schema.items()
            )
            return HTMLResponse(
                f"<html><body style='background:#111;color:#fff;'><h3>Tables in dataset '{chosen_dataset}':</h3><ul>{items_html}</ul></body></html>"
            )
        # ----------- Chart Generation -----------
        # Inside process_prompt(), around where chart is generated (generate_chart intent)

        elif intent == "generate_chart":
            logger.info("🔍 Generating chart...")

            # Get dataset info
            datasets = list_datasets_in_workspace(powerbi_token, workspace_id)
            dataset_names = [ds["name"] for ds in datasets]
            chosen_dataset = choose_relevant_dataset(req.prompt, dataset_names)
            dataset_info = next(
                (ds for ds in datasets if ds["name"] == chosen_dataset), None
            )

            if not dataset_info:
                raise HTTPException(
                    status_code=404, detail="No matching dataset found."
                )

            dataset_id = dataset_info["id"]
            logger.info(f"📊 Using dataset: {chosen_dataset} (ID: {dataset_id})")

            # Determine dataset type (semantic vs push)
            dtype = get_dataset_type(dataset_info)
            logger.info(f"Detected dataset type: {dtype}")

            # Discover schema dynamically with cache
            schema = get_semantic_model_schema(dataset_id, powerbi_token)
            schema_context = build_schema_context(schema)

            # Let LLM choose best table
            chosen_table = choose_best_match(req.prompt, schema_context, "table")
            columns_for_llm = get_columns_for_table(
                chosen_table, dataset_id, powerbi_token
            )

            # Get table columns
            columns = schema.get(chosen_table, [])
            logger.info(f"Using columns for table '{chosen_table}': {columns[:10]}...")

            # Generate DAX plan
            llm_plan = get_llm_plan(req.prompt, columns_for_llm, chosen_table)
            dax_queries = llm_plan.get("dax_queries", [])

            if not dax_queries:
                raise HTTPException(status_code=400, detail="No DAX queries generated.")

            # Execute queries
            df_final = fetch_chart_data(
                dax_queries,
                dataset_id,
                powerbi_token,
                dataset_info,
                chosen_table=chosen_table,
            )

            # --- Generate chart (default to bar if unspecified)
            chart_type = llm_plan.get("chart_type", "bar_chart").lower()
            logger.info(f"Rendering chart type: {chart_type}")

            if chart_type in ("bar", "bar_chart"):
                fig = px.bar(df_final, x=df_final.columns[0], y=df_final.columns[1])
            elif chart_type in ("line", "line_chart"):
                fig = px.line(df_final, x=df_final.columns[0], y=df_final.columns[1])
            elif chart_type in ("pie", "pie_chart"):
                fig = px.pie(
                    df_final, names=df_final.columns[0], values=df_final.columns[1]
                )
            else:
                logger.warning(
                    f"Unknown chart type '{chart_type}', defaulting to bar chart."
                )
                fig = px.bar(df_final, x=df_final.columns[0], y=df_final.columns[1])

            chart_html = fig.to_html(include_plotlyjs="cdn")

            # --- Summarize the chart for narration or text response
            try:
                summary_prompt = f"Summarize this chart in one or two sentences for voice narration:\n\nData sample:\n{df_final.head(10).to_string(index=False)}"
                headers = {
                    "api-key": AZURE_OPENAI_API_KEY,
                    "Content-Type": "application/json",
                }
                body = {
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a Power BI narrator. Describe the insights in a friendly, concise tone.",
                        },
                        {"role": "user", "content": summary_prompt},
                    ],
                    "temperature": 0.3,
                }
                api_version = "2024-02-01"
                url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{GPT_MODEL}/chat/completions?api-version={api_version}"
                resp = requests.post(url, headers=headers, json=body)
                resp.raise_for_status()
                summary_text = resp.json()["choices"][0]["message"]["content"].strip()
            except Exception as e:
                logger.warning(f"Failed to summarize chart: {e}")
                summary_text = f"Chart generated successfully for: {req.prompt}."

            # ✅ Return structured response for chart + voice mode
            return JSONResponse(
                {
                    "type": "chart_with_summary",
                    "summary": summary_text,
                    "chart_html": chart_html,
                }
            )

    except HTTPException as http_ex:
        logger.warning(f"HTTPException: {http_ex.detail}")
        raise http_ex
    except Exception as e:
        logger.exception("Unexpected error during process_prompt.")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
