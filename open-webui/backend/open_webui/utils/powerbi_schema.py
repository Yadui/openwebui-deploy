from pathlib import Path
import json

BASE_DIR = Path(__file__).resolve().parent  # backend/open_webui/utils/
LOCAL_SCHEMA_DIR = BASE_DIR / "powerbi_schemas"  # always writable locally
AZURE_SCHEMA_DIR = Path("/app/backend/data/powerbi_schemas")  # for production


def _schema_path(dataset_id: str) -> Path:
    # Prefer local dir first (dev mode)
    for base in [LOCAL_SCHEMA_DIR, AZURE_SCHEMA_DIR]:
        try:
            base.mkdir(parents=True, exist_ok=True)
            return base / f"{dataset_id}.json"
        except Exception:
            # Ignore permission errors on /app when running locally
            continue

    # Fallback — this should never happen
    return LOCAL_SCHEMA_DIR / f"{dataset_id}.json"


def save_schema(dataset_id: str, schema: dict):
    path = _schema_path(dataset_id)
    path.write_text(json.dumps(schema, indent=2))


def load_schema(dataset_id: str) -> dict | None:
    path = _schema_path(dataset_id)
    if path.exists():
        return json.loads(path.read_text())
    return None
