import httpx
import asyncio
import logging
import difflib
import re
import os
import json
import pandas as pd
import html
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from msal import ConfidentialClientApplication
import plotly.express as px
from time import time, sleep
from typing import Optional, List, Dict, Any, Tuple
from functools import wraps
from pathlib import Path

# ======================
# ENV & LOGGING
# ======================
load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("powerbi_backend")


# ======================
# CONFIG & CACHE
# ======================
MAX_PBI_RETRIES = int(os.getenv("MAX_PBI_RETRIES", "2"))
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
POWERBI_WORKSPACE_ID = os.getenv("POWERBI_WORKSPACE_ID")  # Used as default
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
GPT_MODEL = os.getenv("GPT_MODEL", "gpt-4o")
POWERBI_SCOPE = os.getenv(
    "POWERBI_SCOPE", "https://analysis.windows.net/powerbi/api/.default"
)

# Simple in-memory cache for schema
SCHEMA_CACHE: Dict[str, Tuple[float, dict]] = {}
SCHEMA_CACHE_TTL = int(os.getenv("SCHEMA_CACHE_TTL", "3600"))  # seconds

# Column Cache (Memory + Persistent JSON)
COLUMN_CACHE_FILE = Path(__file__).parent / "cache" / "column_cache.json"
COLUMN_CACHE_FILE.parent.mkdir(exist_ok=True)
COLUMN_CACHE: Dict[str, List[str]] = {}
if COLUMN_CACHE_FILE.exists():
    try:
        with open(COLUMN_CACHE_FILE, "r") as f:
            COLUMN_CACHE = json.load(f)
        logger.info(
            f"📂 Loaded column cache from {COLUMN_CACHE_FILE} ({len(COLUMN_CACHE)} tables)."
        )
    except Exception as e:
        logger.warning(f"⚠️ Failed to load column cache file: {e}")

# --- Startup Check ---
required_vars = {
    "TENANT_ID": TENANT_ID,
    "CLIENT_ID": CLIENT_ID,
    "CLIENT_SECRET": CLIENT_SECRET,
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
# FASTAPI APP
# ======================
app = FastAPI(title="Power BI Visualization Service")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
logger.info("🚀 FastAPI app initialized with CORS.")


# ======================
# BASE MODELS
# ======================
class PromptRequest(BaseModel):
    prompt: str
    metadata: dict | None = None


class ToolRequest(BaseModel):
    prompt: str
    workspace_id: str | None = None
    dataset_id: str | None = None
    metadata: dict | None = None


# ======================
# ASYNC HTTP & API HELPERS
# ======================


async def _call_powerbi_api(
    method: str,
    url: str,
    token: str,
    json_data: Optional[dict] = None,
    max_retries: int = MAX_PBI_RETRIES,
) -> Dict[str, Any]:
    """Async helper to call Power BI API with httpx and retries."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    last_exc = None
    async with httpx.AsyncClient() as client:
        for attempt in range(1, max_retries + 1):
            try:
                if method.upper() == "GET":
                    resp = await client.get(url, headers=headers, timeout=30)
                else:
                    resp = await client.post(
                        url, headers=headers, json=json_data, timeout=30
                    )
                resp.raise_for_status()
                try:
                    return resp.json()
                except ValueError:
                    return {"raw_text": resp.text}
            except (httpx.RequestError, httpx.HTTPStatusError) as e:
                last_exc = e
                logger.warning(
                    f"Power BI API call failed (attempt {attempt}/{max_retries}): {e}"
                )
                await asyncio.sleep(0.5 * attempt)
                continue
    status_code = getattr(last_exc, "response", None) and getattr(
        last_exc.response, "status_code", 500
    )
    raise HTTPException(
        status_code=status_code or 500,
        detail=f"Power BI API Error after {max_retries} attempts: {last_exc}",
    )


async def _call_azure_openai_api(
    system_content: str, user_content: str, is_json: bool = False
) -> Dict[str, Any]:
    """Async helper to call Azure OpenAI."""
    headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}
    body = {
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0,
    }
    if is_json:
        body["response_format"] = {"type": "json_object"}

    api_version = "2024-02-01"
    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{GPT_MODEL}/chat/completions?api-version={api_version}"

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, headers=headers, json=body, timeout=60.0)
            resp.raise_for_status()
            return resp.json()
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logger.error(f"Azure OpenAI API request failed: {e}")
        status_code = getattr(e, "response", None) and getattr(
            e.response, "status_code", 500
        )
        raise HTTPException(
            status_code=status_code or 500, detail=f"Azure OpenAI Error: {str(e)}"
        ) from e


# ======================
# AUTH (OBO)
# ======================
def get_obo_token(user_token: str) -> str:
    """Performs On-Behalf-Of flow. This is SYNC but fast."""
    if not user_token or len(user_token) < 50:
        logger.warning("No valid user token, using client_credentials fallback.")
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

    # Use the general scope for OBO
    obo_scopes = [POWERBI_SCOPE]
    result = app.acquire_token_on_behalf_of(
        user_assertion=user_token, scopes=obo_scopes
    )

    if "access_token" in result:
        logger.info("OBO token acquisition successful.")
        return result["access_token"]
    else:
        error_description = result.get("error_description", "Unknown OBO error.")
        logger.error(f"OBO flow failed: {error_description}")
        raise HTTPException(
            status_code=401,
            detail=f"Could not acquire token on behalf of user: {error_description}",
        )


# ======================
# POWER BI API HELPERS (ASYNC)
# ======================
async def list_workspaces(token: str) -> List[Dict[str, str]]:
    url = "https://api.powerbi.com/v1.0/myorg/groups"
    logger.info("Listing workspaces.")
    data = await _call_powerbi_api("GET", url, token)
    workspaces = data.get("value", [])
    logger.info(f"Found {len(workspaces)} workspaces.")
    # Return name and ID, as needed by frontend
    return [{"name": ws["name"], "id": ws["id"]} for ws in workspaces]


async def list_datasets_in_workspace(
    token: str, workspace_id: str
) -> List[Dict[str, str]]:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    logger.info(f"Listing datasets in workspace: {workspace_id}")
    data = await _call_powerbi_api("GET", url, token)
    datasets = data.get("value", [])
    logger.info(f"Found {len(datasets)} datasets in {workspace_id}.")
    # Return full info for type-checking
    return datasets


async def query_dataset(dax_query: str, dataset_id: str, token: str, workspace_id: str):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
    body = {"queries": [{"query": dax_query}]}
    logger.info(f"Executing DAX query on dataset: {dataset_id}")
    logger.debug(f"DAX Query: {dax_query}")
    return await _call_powerbi_api("POST", url, token, json_data=body)


async def list_dataset_tables(
    dataset_id: str, token: str, workspace_id: str
) -> List[str]:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables"
    try:
        data = await _call_powerbi_api("GET", url, token)
        tables = [t["name"] for t in data.get("value", [])]
        logger.info(f"📘 Found {len(tables)} tables in dataset {dataset_id}: {tables}")
        return tables
    except Exception as e:
        logger.error(f"Failed to list tables for dataset {dataset_id}: {e}")
        return []


async def get_dataset_type(dataset_info: dict) -> str:
    # This logic is fast and doesn't need to be async
    name = dataset_info.get("name", "unknown")
    if dataset_info.get("addRowsAPIEnabled"):
        logger.info(f"📤 Dataset '{name}' detected as Push dataset.")
        return "push"
    logger.info(f"🧠 Dataset '{name}' inferred as Semantic dataset.")
    return "semantic"


async def list_push_dataset_tables(
    dataset_id: str, token: str, workspace_id: str
) -> List[str]:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables"
    try:
        data = await _call_powerbi_api("GET", url, token)
        tables = [tbl["name"] for tbl in data.get("value", [])]
        return tables
    except Exception as e:
        logger.error(f"Failed to list tables for push dataset {dataset_id}: {e}")
        raise HTTPException(
            status_code=500, detail="Failed to list push dataset tables."
        )


async def get_push_dataset_rows(
    dataset_id: str, table_name: str, token: str, workspace_id: str, limit: int = 50
) -> List[dict]:
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/rows"
    try:
        data = await _call_powerbi_api("GET", url, token)
        rows = data.get("value", [])
        return rows[:limit]
    except Exception as e:
        logger.error(
            f"Failed to get rows for push dataset {dataset_id}, table {table_name}: {e}"
        )
        raise HTTPException(status_code=500, detail="Failed to get push dataset rows.")


# ======================
# SCHEMA DISCOVERY (ASYNC)
# ======================


async def save_column_cache():
    """Write current COLUMN_CACHE to JSON file in a thread."""
    try:
        json_data = json.dumps(COLUMN_CACHE, indent=2)
        await run_in_threadpool(COLUMN_CACHE_FILE.write_text, json_data)
        logger.info(f"💾 Column cache saved to {COLUMN_CACHE_FILE}")
    except Exception as e:
        logger.error(f"❌ Failed to save column cache: {e}")


async def get_schema_via_evaluate(
    dataset_id: str, token: str, workspace_id: str
) -> dict:
    logger.info(f"Running EVALUATE-based schema discovery for dataset: {dataset_id}")
    schema = {}
    candidate_tables = await list_dataset_tables(dataset_id, token, workspace_id)

    for table in candidate_tables:
        if table in COLUMN_CACHE:
            logger.info(f"🗃️ Using cached columns for table '{table}'.")
            schema[table] = COLUMN_CACHE[table]
            continue  # Move to next table

        dax_query = f"EVALUATE TOPN(1, '{table}')"
        try:
            result = await query_dataset(dax_query, dataset_id, token, workspace_id)
            tables = result.get("results", [])[0].get("tables", [])
            if not tables or not tables[0].get("rows"):
                continue

            sample_row = tables[0]["rows"][0]
            cols = [key.split("[", 1)[1].rstrip("]") for key in sample_row.keys()]
            if cols:
                schema[table] = cols
                COLUMN_CACHE[table] = cols
                logger.info(f"✅ Found table '{table}' with {len(cols)} columns.")
                await save_column_cache()
        except Exception as e:
            logger.warning(f"Skipping table '{table}' (not found or inaccessible): {e}")
            continue

    if not schema:
        raise HTTPException(
            status_code=404,
            detail="No tables could be discovered via EVALUATE fallback.",
        )
    return schema


async def get_semantic_model_schema(
    dataset_id: str, token: str, workspace_id: str
) -> dict:
    now = time()
    cached = SCHEMA_CACHE.get(dataset_id)
    if cached and (now - cached[0]) < SCHEMA_CACHE_TTL:
        logger.info(f"Using cached schema for dataset {dataset_id}")
        return cached[1]

    logger.info(f"🔍 Discovering schema for dataset: {dataset_id}")
    schema: dict = {}
    base_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"

    # Try TMSCHEMA DMVs first
    tables_body = {"queries": [{"query": "EVALUATE $SYSTEM.TMSCHEMA_TABLES"}]}
    try:
        resp = await _call_powerbi_api("POST", base_url, token, json_data=tables_body)
        rows = resp.get("results", [])[0].get("tables", [])[0].get("rows", [])
        if not rows:
            raise Exception("TMSCHEMA_TABLES returned no rows.")

        table_ids = {
            row["[Name]"]: row["[ID]"]
            for row in rows
            if not row["[Name]"].startswith("LocalDateTable_")
        }

        cols_body = {"queries": [{"query": "EVALUATE $SYSTEM.TMSCHEMA_COLUMNS"}]}
        cols_resp = await _call_powerbi_api(
            "POST", base_url, token, json_data=cols_body
        )
        cols_rows = cols_resp.get("results", [])[0].get("tables", [])[0].get("rows", [])

        # Map columns to tables
        table_cols_map = {tbl_id: [] for tbl_id in table_ids.values()}
        for col in cols_rows:
            tbl_id = col["[TableID]"]
            if tbl_id in table_cols_map:
                table_cols_map[tbl_id].append(col["[Name]"])

        # Build final schema
        for tbl_name, tbl_id in table_ids.items():
            schema[tbl_name] = table_cols_map[tbl_id]

        SCHEMA_CACHE[dataset_id] = (now, schema)
        logger.info(
            f"✅ Retrieved schema with {len(schema)} tables from dataset {dataset_id} via DMV"
        )
        return schema

    except Exception as e:
        logger.warning(f"DMV schema discovery failed ({e}). Falling back to EVALUATE.")
        try:
            schema = await get_schema_via_evaluate(dataset_id, token, workspace_id)
            SCHEMA_CACHE[dataset_id] = (now, schema)
            return schema
        except Exception as eval_error:
            logger.exception(f"EVALUATE fallback failed: {eval_error}")
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve semantic model schema (all fallbacks failed).",
            )


async def get_columns_for_table(
    chosen_table: str, dataset_id: str, token: str, workspace_id: str
) -> List[str]:
    """Get columns for a given table (from cache or live query)."""
    if chosen_table in COLUMN_CACHE:
        logger.info(f"🗃️ Using cached columns for table '{chosen_table}'")
        return COLUMN_CACHE[chosen_table]

    logger.info(f"🔍 Fetching columns for table '{chosen_table}' via TOPN(1)")
    try:
        dax = f"EVALUATE TOPN(1, '{chosen_table}')"
        result = await query_dataset(dax, dataset_id, token, workspace_id)
        results_list = result.get("results", [])
        if results_list:
            tables = results_list[0].get("tables", [])
            if tables and tables[0].get("rows"):
                sample_row = tables[0]["rows"][0]
                columns = [
                    key.split("[", 1)[1].rstrip("]") for key in sample_row.keys()
                ]
                COLUMN_CACHE[chosen_table] = columns
                await save_column_cache()
                logger.info(
                    f"✅ Added {len(columns)} columns for table '{chosen_table}' to cache."
                )
                return columns
    except Exception as e:
        logger.warning(f"⚠️ Failed to fetch columns for '{chosen_table}': {e}")
    return []


# ======================
# DAX VALIDATION & CORRECTION (SYNC)
# ======================
def validate_and_correct_columns(dax_query: str, chosen_table: str) -> Optional[str]:
    """Validates and corrects column names in a DAX query. (From Snippet 2)"""
    logger.info(f"🔍 Validating DAX query for table '{chosen_table}': {dax_query}")
    columns_in_query = set(re.findall(r"(?:[A-Za-z0-9_]+)?\[(.*?)\]", dax_query))
    valid_columns = set(COLUMN_CACHE.get(chosen_table, []))
    if not valid_columns:
        logger.warning(
            f"No columns in cache for table '{chosen_table}', skipping validation."
        )
        return dax_query  # Cannot validate

    corrected_query = dax_query
    skip_query = False
    replacements_made = 0

    alias_pattern = re.findall(r'"([^"]+)"\s*,\s*SUM\(', dax_query)
    alias_columns = set(alias_pattern)

    for col in columns_in_query:
        if col not in valid_columns and col not in alias_columns:
            matches = difflib.get_close_matches(
                col, valid_columns, n=1, cutoff=0.75
            )  # Stricter cutoff
            if matches:
                replacement = matches[0]
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

    # Auto-correct invalid SUMMARIZE
    summarize_pattern = re.compile(
        r"SUMMARIZE\s*\(([^,]+?),\s*([^,]+?),\s*\"([^\"]+)\",\s*SUM\(([^)]+)\)\s*\)",
        re.IGNORECASE,
    )
    if summarize_pattern.search(corrected_query):
        corrected_query = summarize_pattern.sub(
            r"ADDCOLUMNS(SUMMARIZE(\1, \2), \"\3\", SUM(\4))", corrected_query
        )
        logger.warning("⚙️ Auto-corrected invalid DAX SUMMARIZE to ADDCOLUMNS.")

    # Auto-correct TOPN sort
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
        logger.warning(f"⚙️ Fixed TOPN sort: replaced [{sort_col}] with [{alias}]")

    logger.info(
        f"✅ Validated {len(columns_in_query)} columns, {replacements_made} replacements."
    )
    return None if skip_query else corrected_query


# ======================
# LLM HELPERS (ASYNC)
# ======================
async def choose_best_match(prompt: str, candidates: list[str], label: str) -> str:
    logger.info(f"Asking LLM to select best {label} for: '{prompt}'")
    system_msg = (
        f"You are a Power BI expert. The user asked: '{prompt}'."
        f" From the following list of {label}s, choose the one most relevant."
        f" Respond ONLY with the exact name from the list."
    )
    user_msg = f"Available {label}s: {candidates}"

    try:
        resp = await _call_azure_openai_api(system_msg, user_msg)
        choice = resp["choices"][0]["message"]["content"].strip().strip('"').strip("'")

        def normalize_name(x: str) -> str:
            if "Table:" in x:
                return x.split("Table:")[1].split("|")[0].strip()
            return x.strip()

        normalized_candidates = [normalize_name(c) for c in candidates]
        normalized_choice = normalize_name(choice)

        if normalized_choice not in normalized_candidates:
            logger.warning(
                f"LLM chose invalid {label} '{choice}', defaulting to first."
            )
            return normalized_candidates[0]
        logger.info(f"✅ Selected {label}: {normalized_choice}")
        return normalized_choice
    except Exception as e:
        logger.error(f"LLM failed to choose {label}: {e}")
        return candidates[0] if candidates else None


async def get_llm_plan(prompt, columns, table_name) -> Dict[str, Any]:
    logger.info("Asking LLM to generate DAX plan.")
    context_hint = ""
    if table_name:
        context_hint += f"Table: {table_name}. "
    if columns:
        sample_cols = ", ".join(columns[:20])
        context_hint += f"Columns: {sample_cols}. "

    system_content = (
        "You are a strict Power BI DAX code generator used inside a backend tool. "
        "You MUST respond ONLY with a valid JSON object. "
        "Never include natural language, explanations, commentary, or markdown. "
        "Never say you cannot create charts. "
        "Always output JSON with this structure: "
        "{ 'intent': 'generate_chart' | 'list_workspaces' | 'list_datasets' | 'list_tables', "
        "  'chart_type': 'bar' | 'line' | 'pie', "
        "  'dax_queries': ['EVALUATE ...'] }. "
        "When generating DAX, output ONLY executable DAX. "
        "NEVER include English sentences, comments, descriptions, or backticks. "
        "Ensure all DAX queries return tabular outputs (use SELECTCOLUMNS or SUMMARIZECOLUMNS as needed)."
    )
    user_content = f"Context: {context_hint}\nUser: {prompt}"

    try:
        resp = await _call_azure_openai_api(system_content, user_content, is_json=True)
        content = resp["choices"][0]["message"]["content"]
        # Sanitize accidental markdown or explanations
        content = content.replace("```", "").strip()
        plan = json.loads(content)
        logger.info(f"LLM generated plan with intent: '{plan.get('intent')}'")
        return plan
    except json.JSONDecodeError as e:
        raw = resp.get("choices", [{}])[0].get("message", {}).get("content", "")
        logger.error(f"Failed JSON decode for plan: {e}. Raw: {raw}")
        raise HTTPException(
            status_code=500, detail="Internal error: LLM plan invalid JSON."
        )


# ======================
# ORCHESTRATION (ASYNC)
# ======================
async def fetch_chart_data(
    dax_queries: list,
    dataset_id: str,
    token: str,
    workspace_id: str,
    dataset_info: dict,
    chosen_table: str,
) -> pd.DataFrame:
    logger.info(f"Fetching chart data from dataset {dataset_id}")
    all_dfs = []
    dtype = await get_dataset_type(dataset_info)
    is_push = dtype == "push"

    for i, q in enumerate(dax_queries):
        # Validate and correct columns (this is fast/sync)
        corrected_q = validate_and_correct_columns(q, chosen_table)
        if corrected_q is None:
            logger.error(f"Skipping DAX query {i + 1} due to column errors.")
            continue

        try:
            result = await query_dataset(corrected_q, dataset_id, token, workspace_id)
            tables = result.get("results", [])[0].get("tables", [])
            if not tables or not tables[0].get("rows"):
                logger.error("Power BI returned no rows or malformed result.")
                continue
            if tables and tables[0].get("rows"):
                all_dfs.append(pd.DataFrame(tables[0]["rows"]))
            else:
                logger.warning(f"Query {i + 1} returned no tables: {corrected_q}")

        except Exception as dex:
            logger.warning(f"DAX query failed: {dex}. Checking for Push fallback.")
            if is_push:
                try:
                    rows = await get_push_dataset_rows(
                        dataset_id, chosen_table, token, workspace_id
                    )
                    if rows:
                        all_dfs.append(pd.DataFrame(rows))
                        logger.info("Push dataset fallback data loaded.")
                except Exception as fallback_ex:
                    logger.error(f"Push dataset fallback failed: {fallback_ex}")
            else:
                raise  # Re-raise DAX error if not a push dataset

    if not all_dfs:
        raise HTTPException(
            status_code=404,
            detail="No data returned from Power BI for the generated queries.",
        )

    df_final = pd.concat(all_dfs, ignore_index=True)
    if df_final.empty:
        raise HTTPException(status_code=404, detail="Power BI returned an empty table.")
    logger.info(f"📊 Final DataFrame shape: {df_final.shape}")
    return df_final


async def process_prompt(
    req: ToolRequest,
    powerbi_token: str,
    workspace_id: str,
    dataset_id: str | None,
) -> JSONResponse:
    logger.info(f"Processing prompt: '{req.prompt}'")

    # 1. Get initial plan
    llm_plan = await get_llm_plan(req.prompt, [], "")
    intent = llm_plan.get("intent", "list_datasets")
    logger.info(f"Determined intent: {intent}")

    # --- Simple intents (no dataset needed) ---
    if intent == "list_workspaces":
        workspaces = await list_workspaces(powerbi_token)
        items_html = "".join(f"<li>{html.escape(ws['name'])}</li>" for ws in workspaces)
        return JSONResponse(
            {
                "type": "html",
                "content": f"<h3>Available Workspaces:</h3><ul>{items_html}</ul>",
            }
        )

    # --- Intents requiring a workspace ---
    if not workspace_id:
        workspace_id = POWERBI_WORKSPACE_ID  # Fallback to default
        if not workspace_id:
            raise HTTPException(
                status_code=400, detail="No workspace ID provided or configured."
            )

    if intent == "list_datasets":
        datasets = await list_datasets_in_workspace(powerbi_token, workspace_id)
        dataset_names = [ds["name"] for ds in datasets]
        items_html = "".join(f"<li>{html.escape(name)}</li>" for name in dataset_names)
        return JSONResponse(
            {
                "type": "html",
                "content": f"<h3>Available Datasets:</h3><ul>{items_html}</ul>",
            }
        )

    # --- Intents requiring a dataset ---
    if not dataset_id:
        datasets = await list_datasets_in_workspace(powerbi_token, workspace_id)
        dataset_names = [ds["name"] for ds in datasets]
        chosen_dataset_name = await choose_best_match(
            req.prompt, dataset_names, "dataset"
        )
        dataset_info = next(
            (d for d in datasets if d["name"] == chosen_dataset_name), None
        )
    else:
        # Fetch info for the specific dataset_id
        datasets = await list_datasets_in_workspace(powerbi_token, workspace_id)
        dataset_info = next((d for d in datasets if d["id"] == dataset_id), None)

    if not dataset_info:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    dataset_id = dataset_info["id"]  # Ensure dataset_id is set
    dtype = await get_dataset_type(dataset_info)

    if intent == "list_tables":
        if dtype == "semantic":
            schema = await get_semantic_model_schema(
                dataset_id, powerbi_token, workspace_id
            )
        else:  # push
            tables = await list_push_dataset_tables(
                dataset_id, powerbi_token, workspace_id
            )
            schema = {tbl: [] for tbl in tables}

        items_html = "".join(
            f"<li>{html.escape(tbl)} ({len(cols)} cols)</li>"
            for tbl, cols in schema.items()
        )
        return JSONResponse(
            {
                "type": "html",
                "content": f"<h3>Tables in {dataset_info['name']}:</h3><ul>{items_html}</ul>",
            }
        )

    # --- Chart Generation ---
    elif intent == "generate_chart":
        logger.info("🔍 Generating chart...")

        # Discover schema
        if dtype == "semantic":
            schema = await get_semantic_model_schema(
                dataset_id, powerbi_token, workspace_id
            )
        else:  # push
            tables = await list_push_dataset_tables(
                dataset_id, powerbi_token, workspace_id
            )
            schema = {}
            for tbl in tables:
                schema[tbl] = await get_columns_for_table(
                    tbl, dataset_id, powerbi_token, workspace_id
                )

        if not schema:
            raise HTTPException(
                status_code=404,
                detail="Could not find any tables or schema for this dataset.",
            )

        schema_context = [
            f"Table: {tbl} | Columns: {', '.join(cols[:20])}"
            for tbl, cols in schema.items()
        ]
        chosen_table = await choose_best_match(req.prompt, schema_context, "table")
        columns_for_llm = schema.get(chosen_table, [])

        # Get final DAX plan with schema context
        llm_plan = await get_llm_plan(req.prompt, columns_for_llm, chosen_table)
        dax_queries = llm_plan.get("dax_queries", [])
        if not dax_queries:
            raise HTTPException(status_code=400, detail="No DAX queries generated.")

        # Execute queries
        df_final = await fetch_chart_data(
            dax_queries,
            dataset_id,
            powerbi_token,
            workspace_id,
            dataset_info,
            chosen_table,
        )

        # Insert safety check for chartable columns
        if df_final.shape[1] < 2:
            logger.error("DataFrame has insufficient columns to chart.")
            raise HTTPException(status_code=400, detail="DAX result does not contain chartable columns.")

        # Generate chart
        chart_type = llm_plan.get("chart_type", "bar_chart").lower()
        logger.info(f"Rendering chart type: {chart_type}")

        try:
            if chart_type in ("bar", "bar_chart"):
                fig = px.bar(df_final, x=df_final.columns[0], y=df_final.columns[1])
            elif chart_type in ("line", "line_chart"):
                fig = px.line(df_final, x=df_final.columns[0], y=df_final.columns[1])
            elif chart_type in ("pie", "pie_chart"):
                fig = px.pie(
                    df_final, names=df_final.columns[0], values=df_final.columns[1]
                )
            else:
                fig = px.bar(df_final, x=df_final.columns[0], y=df_final.columns[1])
            chart_html = fig.to_html(include_plotlyjs="cdn")
        except Exception as e:
            logger.error(f"Failed to generate Plotly chart: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to render chart: {e}")

        # Summarize chart
        try:
            summary_prompt = f"Summarize this chart in one sentence for voice narration:\nData:\n{df_final.head(5).to_string()}"
            summary_resp = await _call_azure_openai_api(
                "You are a concise data narrator.", summary_prompt
            )
            summary_text = summary_resp["choices"][0]["message"]["content"].strip()
        except Exception as e:
            logger.warning(f"Failed to summarize chart: {e}")
            summary_text = f"Chart generated for: {req.prompt}."

        return JSONResponse(
            {
                "type": "chart_with_summary",
                "summary": summary_text,
                "chart_html": chart_html,
            }
        )

    else:
        raise HTTPException(status_code=400, detail=f"Unknown intent: {intent}")


# ======================
# API ENDPOINTS (ASYNC)
# ======================


async def get_token_from_request(request: Request) -> str:
    """Dependency to extract and validate token."""
    auth_header = request.headers.get("Authorization")
    cookie_token = request.cookies.get("access_token") or request.cookies.get(
        "teams_token"
    )

    user_token = None
    if auth_header and auth_header.startswith("Bearer "):
        user_token = auth_header.split(" ")[1]
    elif cookie_token:
        user_token = cookie_token

    if not user_token:
        logger.error("No authorization token found in headers or cookies.")
        raise HTTPException(status_code=401, detail="Authentication token missing.")

    # Run sync OBO in a threadpool to not block the server
    try:
        powerbi_token = await run_in_threadpool(get_obo_token, user_token)
        return powerbi_token
    except Exception as e:
        logger.error(f"Tool call failed due to OBO token acquisition: {e}")
        raise HTTPException(
            status_code=401,
            detail=f"Authentication error: {getattr(e, 'detail', str(e))}",
        ) from e


@app.get("/powerbi/workspaces", response_model=List[Dict[str, str]])
async def get_workspaces(token: str = Depends(get_token_from_request)):
    """Endpoint for frontend to list workspaces."""
    return await list_workspaces(token)


@app.get(
    "/powerbi/workspaces/{workspace_id}/datasets", response_model=List[Dict[str, Any]]
)
async def get_datasets(workspace_id: str, token: str = Depends(get_token_from_request)):
    """Endpoint for frontend to list datasets."""
    return await list_datasets_in_workspace(token, workspace_id)


@app.post("/auth/tool/powerbi")
async def handle_tool_call(
    req: ToolRequest, token: str = Depends(get_token_from_request)
):
    """Main endpoint for the LLM tool call."""
    logger.info(f"Received tool call for prompt: '{req.prompt}'")

    workspace_id = req.workspace_id
    dataset_id = req.dataset_id
    if req.metadata:
        workspace_id = workspace_id or req.metadata.get("powerbi_workspace_id")
        dataset_id = dataset_id or req.metadata.get("powerbi_dataset_id")

    if not workspace_id:
        workspace_id = POWERBI_WORKSPACE_ID  # Fallback

    if not workspace_id:
        raise HTTPException(
            status_code=400,
            detail="No Power BI Workspace ID was provided or configured.",
        )

    try:
        return await process_prompt(req, token, workspace_id, dataset_id)
    except HTTPException as http_ex:
        logger.warning(f"HTTPException in tool: {http_ex.detail}")
        # Return a JSON error message that the LLM can understand
        return JSONResponse(
            status_code=http_ex.status_code, content={"error": http_ex.detail}
        )
    except Exception as e:
        logger.exception("Unexpected error during tool processing.")
        return JSONResponse(
            status_code=500, content={"error": f"Internal server error: {str(e)}"}
        )


# Health check
@app.get("/health")
def health_check():
    return {"status": "ok"}
