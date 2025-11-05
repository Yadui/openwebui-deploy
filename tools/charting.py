# powerbi_viz_tool.py
import logging
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

# ======================
# ENV & LOGGING
load_dotenv()


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("powerbi_backend")

# ======================
# CONFIG
# ======================
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


def query_dataset(dax_query: str, dataset_id: str, token: str):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{POWERBI_WORKSPACE_ID}/datasets/{dataset_id}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"queries": [{"query": dax_query}]}
    logger.info(f"Executing DAX query on dataset: {dataset_id}")
    logger.debug(f"Request URL: {url}")
    logger.debug(f"DAX Query: {dax_query}")
    try:
        resp = requests.post(url, headers=headers, json=body)
        resp.raise_for_status()  # Crucial check
        logger.info(f"DAX query executed successfully.")
        logger.debug(
            f"DAX response snippet: {str(resp.content)[:200]}..."
        )  # Log a snippet
        return resp.json()
    except requests.exceptions.RequestException as e:
        # Log the specific error from Power BI if available
        error_detail = str(e)
        if hasattr(e, "response") and e.response is not None:
            try:
                error_content = e.response.json()
                error_detail = (
                    f"{e.response.status_code} {e.response.reason}: {error_content}"
                )
            except json.JSONDecodeError:
                error_detail = (
                    f"{e.response.status_code} {e.response.reason}: {e.response.text}"
                )
        logger.error(f"Power BI API request failed for DAX query: {error_detail}")
        logger.error(f"Failed DAX Query was: {dax_query}")  # Log the failed query
        raise HTTPException(
            status_code=e.response.status_code
            if hasattr(e, "response") and e.response is not None
            else 500,
            detail=f"Power BI DAX Error: {error_detail}",
        ) from e


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


def guess_relevant_table(prompt: str, dataset_name: str) -> str:
    """Uses LLM to guess the most likely table name within a dataset for the given prompt."""
    logger.info(f"Asking LLM to guess relevant table for dataset '{dataset_name}'.")
    logger.debug(f"Prompt: '{prompt}'")

    headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}
    # Prompt the LLM to guess based on common patterns and the prompt
    system_content = (
        f"You are an AI assistant. The user wants data related to '{prompt}' from the Power BI dataset named '{dataset_name}'. "
        f"Based on the prompt and dataset name, guess the most likely primary table name within that dataset (e.g., 'Sales', 'Sheet1', 'FactTable', 'Data'). "
        f"Return ONLY the single, most likely table name as a string, without quotes or explanation."
    )
    user_content = "What is the most likely table name to use?"
    body = {
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.1,
    }  # Allow a little creativity
    api_version = "2024-02-01"
    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{GPT_MODEL}/chat/completions?api-version={api_version}"

    try:
        resp = requests.post(url, headers=headers, json=body)
        resp.raise_for_status()
        # Extract the guessed name, remove potential quotes
        guessed_table = (
            resp.json()["choices"][0]["message"]["content"]
            .strip()
            .strip('"')
            .strip("'")
        )
        logger.info(f"LLM guessed table name: '{guessed_table}'")
        # Basic validation: ensure it's not empty
        if not guessed_table:
            logger.error("LLM failed to guess a table name.")
            raise HTTPException(
                status_code=500, detail="Could not determine a table name."
            )
        return guessed_table
    except requests.exceptions.RequestException as e:
        logger.error(f"Azure OpenAI API request failed for guessing table: {e}")
        status_code = (
            e.response.status_code
            if hasattr(e, "response") and e.response is not None
            else 500
        )
        raise HTTPException(
            status_code=status_code,
            detail=f"Azure OpenAI Error guessing table: {str(e)}",
        ) from e
    except Exception as e:
        logger.exception(
            f"Unexpected error while guessing table for dataset '{dataset_name}'."
        )
        raise HTTPException(
            status_code=500, detail="Internal error guessing relevant table."
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


def fetch_chart_data(dax_queries: list, dataset_id: str, token: str):
    logger.info(
        f"Fetching chart data from dataset {dataset_id} using {len(dax_queries)} queries."
    )
    all_dfs = []
    try:
        for i, q in enumerate(dax_queries):
            logger.debug(f"Executing query {i + 1}/{len(dax_queries)}: {q}")
            result = query_dataset(q, dataset_id, token)  # query_dataset logs errors
            tables = result.get("results", [])[0].get("tables", [])
            if tables:
                all_dfs.append(pd.DataFrame(tables[0]["rows"]))
            else:
                logger.warning(f"Query {i + 1} returned no tables: {q}")

        if not all_dfs:
            logger.error("No data returned from Power BI for any DAX query.")
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


def choose_relevant_table(
    prompt: str, dataset_name: str, table_names: list[str]
) -> str:
    logger.info(f"Asking LLM to choose relevant table for dataset '{dataset_name}'.")
    logger.debug(f"Prompt: '{prompt}', Available Tables: {table_names}")
    # Handle the simple case directly
    if len(table_names) == 1:
        logger.info(
            f"Only one table found ('{table_names[0]}'), selecting it automatically."
        )
        return table_names[0]

    headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}
    system_content = f"You are an AI assistant. The user wants data related to '{prompt}' from the Power BI dataset '{dataset_name}'. From the following list of table names, return ONLY the single most relevant table name, and nothing else."
    user_content = (
        f"Available Tables: {json.dumps(table_names)}\n\nMost relevant table name?"
    )
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
        chosen_table = (
            resp.json()["choices"][0]["message"]["content"].strip().strip('"')
        )
        logger.info(f"LLM chose table: '{chosen_table}'")
        # Validate LLM choice
        if chosen_table not in table_names:
            logger.warning(
                f"LLM chose invalid table '{chosen_table}'. Falling back to first."
            )
            return table_names[0]  # Fallback to the first table
        return chosen_table
    except requests.exceptions.RequestException as e:
        logger.error(f"Azure OpenAI API request failed for choosing table: {e}")
        status_code = (
            e.response.status_code
            if hasattr(e, "response") and e.response is not None
            else 500
        )
        raise HTTPException(
            status_code=status_code,
            detail=f"Azure OpenAI Error choosing table: {str(e)}",
        ) from e
    except Exception as e:
        logger.exception(
            f"Unexpected error while choosing table for dataset '{dataset_name}'."
        )
        raise HTTPException(
            status_code=500, detail="Internal error choosing relevant table."
        ) from e


def get_llm_plan(prompt, columns, table_name):
    logger.info("Asking LLM to determine intent and generate DAX plan.")
    logger.debug(f"Prompt: '{prompt}', Table: '{table_name}', Columns: {columns}")
    headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}
    system_content = (
        f"You are a Power BI DAX expert. Determine user intent ('generate_chart', 'list_datasets', 'list_workspaces'). Return ONLY a valid JSON object. "
        f"If 'generate_chart': "
        f"  - Include 'chart_type': Best Plotly type ('bar', 'line', 'pie', 'scatter') based on the prompt. Default to 'bar'. "
        f"  - Include 'dax_queries': LIST of DAX strings starting with 'EVALUATE', using SUMMARIZECOLUMNS and simple aggregations (SUM, COUNT, etc.). "
        f"  - Use table '{table_name}' (in single quotes) and columns {columns} (e.g., '{table_name}'[Column Name]). "
        f'Example: {{"intent": "generate_chart", "chart_type": "bar", "dax_queries": ["EVALUATE ..."], "debug": "..."}}'
    )
    body = {
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    api_version = "2024-02-01"
    url = f"{AZURE_OPENAI_ENDPOINT}/openai/deployments/{GPT_MODEL}/chat/completions?api-version={api_version}"
    try:
        resp = requests.post(url, headers=headers, json=body)
        resp.raise_for_status()
        plan = json.loads(resp.json()["choices"][0]["message"]["content"])
        logger.info(f"LLM generated plan with intent: '{plan.get('intent')}'")
        logger.debug(f"Full LLM plan: {plan}")
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
        raw_response = (
            resp.json()["choices"][0]["message"]["content"] if resp else "No response"
        )
        logger.error(f"Failed JSON decode for plan: {e}. Raw: {raw_response}")
        raise HTTPException(
            status_code=500, detail="Internal error: LLM plan invalid JSON."
        ) from e
    except Exception as e:
        logger.exception("Unexpected error getting LLM plan.")
        raise HTTPException(
            status_code=500, detail="Internal error getting plan."
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


def process_prompt(
    req: PromptRequest,
    powerbi_token: str,
    workspace_id: str | None = None,
    dataset_id: str | None = None,
):
    logger.info(f"Processing prompt: '{req.prompt}'")
    try:
        # Step 1: Get intent from LLM (no schema needed yet)
        llm_plan = get_llm_plan(req.prompt, [], "")
        intent = llm_plan.get("intent")
        logger.info(f"Determined intent: {intent}")

        # Step 2: Handle simple intents (listing)
        if intent == "list_workspaces":
            workspaces = list_workspaces(powerbi_token)
            items_html = "".join(
                [f"<li>{html.escape(name)}</li>" for name in workspaces]
            )
            logger.info("Successfully listed workspaces.")
            return HTMLResponse(
                content=f"<html><head><style>body{{background:#1a1a1a;color:#fff;}}</style></head><body><h2>Available Workspaces:</h2><ul>{items_html}</ul></body></html>"
            )

        elif intent == "list_datasets":
            datasets = list_datasets_in_workspace(powerbi_token)
            dataset_names = [ds["name"] for ds in datasets]
            items_html = "".join(
                [f"<li>{html.escape(name)}</li>" for name in dataset_names]
            )
            logger.info("Successfully listed datasets.")
            return HTMLResponse(
                content=f"<html><head><style>body{{background:#1a1a1a;color:#fff;}}</style></head><body><h2>Available Datasets:</h2><ul>{items_html}</ul></body></html>"
            )

        # Step 3: Handle complex intent (generate_chart)
        elif intent == "generate_chart":
            logger.info("Executing generate_chart intent.")

            # A. Get available datasets
            available_datasets = list_datasets_in_workspace(powerbi_token, workspace_id)
            if not available_datasets:
                raise HTTPException(status_code=404, detail="No datasets found.")

            # B. Choose relevant dataset
            if dataset_id:
                selected_dataset_id = dataset_id
                selected_dataset_name = next(
                    (ds["name"] for ds in available_datasets if ds["id"] == dataset_id),
                    "Unknown Dataset",
                )
                logger.info(
                    f"Using provided dataset: '{selected_dataset_name}' (ID: {dataset_id})"
                )
            else:
                dataset_names = [ds["name"] for ds in available_datasets]
                chosen_dataset_name = choose_relevant_dataset(req.prompt, dataset_names)
                selected_dataset = next(
                    (
                        ds
                        for ds in available_datasets
                        if ds["name"] == chosen_dataset_name
                    ),
                    None,
                )
                if not selected_dataset:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Dataset '{chosen_dataset_name}' not found.",
                    )
                selected_dataset_id = selected_dataset["id"]
                selected_dataset_name = chosen_dataset_name
                logger.info(
                    f"Chosen dataset: '{selected_dataset_name}' (ID: {selected_dataset_id})"
                )

            # C. Guess the table name using LLM
            chosen_table_name = guess_relevant_table(req.prompt, chosen_dataset_name)

            # D. Try to get columns for the guessed table name
            columns = get_table_columns(
                chosen_table_name, selected_dataset_id, powerbi_token
            )

            # E. Get the columns for the chosen table from the schema
            if not columns:
                # If getting columns failed, the guessed table name was likely wrong.
                logger.error(
                    f"Could not retrieve columns for guessed table '{chosen_table_name}' in dataset '{chosen_dataset_name}'."
                )
                # Provide a specific error asking the user to specify the table.
                raise HTTPException(
                    status_code=404,
                    detail=f"Could not find or access table '{chosen_table_name}' in dataset '{chosen_dataset_name}'. Please specify the table name in your prompt if you know it.",
                )
            logger.info(
                f"Using guessed table: '{chosen_table_name}' with columns: {columns}"
            )
            # F. Get the DAX plan using the chosen table and columns
            final_llm_plan = get_llm_plan(req.prompt, columns, chosen_table_name)
            dax_queries = final_llm_plan.get("dax_queries", [])
            if not dax_queries or not isinstance(dax_queries, list):
                logger.error(
                    f"LLM failed to generate valid DAX queries. Plan: {final_llm_plan}"
                )
                raise HTTPException(
                    status_code=400, detail="Failed to generate DAX queries."
                )

            # G. Fetch data using DAX
            df = fetch_chart_data(dax_queries, selected_dataset_id, powerbi_token)
            if df.empty:
                raise HTTPException(status_code=404, detail="Queries returned no data.")

            logger.info("Data retrieved, preparing Plotly chart.")
            df.columns = [c.split("[", 1)[-1].rstrip("]") for c in df.columns]
            if not df.columns.any():
                raise HTTPException(
                    status_code=500, detail="Error processing data columns."
                )

            x_col = df.columns[0]  # Assume first column is X-axis
            y_cols = df.select_dtypes(include="number").columns.tolist()
            if x_col in y_cols:
                y_cols.remove(x_col)
            if not y_cols:
                raise HTTPException(
                    status_code=400, detail="No numeric Y columns found."
                )

            # Get chart type determined by the LLM (default to bar)
            chart_type = final_llm_plan.get("chart_type", "bar").lower()
            logger.info(f"Using chart type: '{chart_type}'")

            fig = None
            title = req.prompt  # Use user prompt as title

            try:
                # Use Plotly Express for easier chart creation
                if chart_type == "bar":
                    fig = px.bar(df, x=x_col, y=y_cols, title=title, barmode="group")
                elif chart_type == "line":
                    # Melt DataFrame for px.line if multiple y_cols
                    if len(y_cols) > 1:
                        df_melted = df.melt(
                            id_vars=[x_col],
                            value_vars=y_cols,
                            var_name="Metric",
                            value_name="Value",
                        )
                        fig = px.line(
                            df_melted,
                            x=x_col,
                            y="Value",
                            color="Metric",
                            title=title,
                            markers=True,
                        )
                    else:
                        fig = px.line(
                            df, x=x_col, y=y_cols[0], title=title, markers=True
                        )
                elif chart_type == "pie":
                    # Pie charts usually work best with one value column
                    fig = px.pie(df, names=x_col, values=y_cols[0], title=title)
                    fig.update_traces(textposition="inside", textinfo="percent+label")
                elif chart_type == "scatter":
                    fig = px.scatter(df, x=x_col, y=y_cols, title=title)
                else:  # Default to bar chart if type is unknown
                    logger.warning(
                        f"Unknown chart type '{chart_type}', defaulting to bar chart."
                    )
                    fig = px.bar(df, x=x_col, y=y_cols, title=title, barmode="group")

                # Apply dark theme styling
                fig.update_layout(
                    template="plotly_dark",
                    paper_bgcolor="#1a1a1a",
                    plot_bgcolor="#1a1a1a",
                    font_color="#fff",
                    margin=dict(t=50, b=50, l=50, r=50),  # Consistent margins
                )

                chart_html = fig.to_html(
                    full_html=False, include_plotlyjs="cdn"
                )  # Get div + script

            except Exception as chart_err:
                logger.exception("Failed to generate Plotly Express chart.")
                raise HTTPException(
                    status_code=500, detail=f"Error generating chart: {str(chart_err)}"
                )

            # Prepare full HTML response with DAX query viewer
            dax_query_string = ";\n".join(dax_queries)
            dax_html = f"""<details style="margin-top: 15px; font-family: sans-serif; color: #fff;"><summary style="cursor: pointer; font-weight: bold;">View DAX Query</summary><pre><code style="background-color: #000; color: #fff; padding: 10px; display: block; border-radius: 5px; white-space: pre-wrap;">{html.escape(dax_query_string)}</code></pre></details>"""

            full_html_doc = f"""
            <!DOCTYPE html><html><head><title>Interactive Chart</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>body{{margin:0; background:#1a1a1a; color:#fff;}}</style></head>
            <body>{chart_html}{dax_html}</body></html>
            """

            logger.info("Successfully generated chart HTML response.")
            return HTMLResponse(
                content=full_html_doc, headers={"Content-Disposition": "inline"}
            )

    except HTTPException as http_ex:
        logger.warning(
            f"HTTPException during processing: {http_ex.status_code} - {http_ex.detail}"
        )
        raise http_ex
    except Exception as e:
        logger.exception("Unexpected error during process_prompt.")
        raise HTTPException(
            status_code=500, detail=f"Internal server error: {str(e)}"
        ) from e
