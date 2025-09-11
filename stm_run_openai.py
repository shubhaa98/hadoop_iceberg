
"""
stm_run_openai.py
Extracts ETL mappings using OpenAI and enriches them with catalog metadata.
"""

import os
import json
import csv
import argparse
from typing import List, Dict, Any
from openai import OpenAI

from dotenv import load_dotenv
load_dotenv()  # this will read from .env automatically

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise EnvironmentError("OPENAI_API_KEY environment variable is not set.")

DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
CATALOG_PATH = "catalog_schema.json"

SCHEMA = {
    "type": "object",
    "properties": {
        "mappings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source_table": {"type": "string"},
                    "source_column": {"type": "string"},
                    "target_table": {"type": "string"},
                    "target_column": {"type": "string"},
                    "transformation_logic": {"type": "string"}
                },
                "required": [
                    "source_table",
                    "source_column",
                    "target_table",
                    "target_column",
                    "transformation_logic"
                ],
                "additionalProperties": False
            }
        }
    },
    "required": ["mappings"],
    "additionalProperties": False
}

SYSTEM_INSTRUCTIONS = (
    "You are a data engineering assistant. You analyze PySpark SQL ETL code "
    "and extract column-level mappings for lineage documentation. "
    "Return only structured content that conforms to the provided JSON schema."
)

USER_TASK_TEMPLATE = """\
Analyze the following PySpark ETL script and extract column-level mappings.

Rules:
- Only extract mappings for the final output table written via `saveAsTable` (e.g., final_db.customer_summary).
- Resolve lineage back to the **original base tables** (staging_db.* or core_db.*), not intermediate views like base_layer, union_layer, or nested_layer.
- For each target column, identify the ultimate source table and column(s) it is derived from.
- If a column is derived using expressions (e.g., CASE, COALESCE, SUM), include the exact SQL logic in `transformation_logic`.
- Fully resolve table names using variables (e.g., stg_db.customer → staging_db.customer).
- Output MUST match the JSON schema (no extra fields).

ETL Script:
----------------
{etl_script}
----------------
"""

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def save_json(data: Dict[str, Any], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved JSON to {path}")

def save_csv(mappings: List[Dict[str, Any]], path: str) -> None:
    fields = [
        "source_table", "source_column", "target_table", "target_column",
        "transformation_logic", "data_type", "is_categorical", "categories"
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in mappings:
            writer.writerow({
                k: ", ".join(row[k]) if isinstance(row[k], list) else row.get(k, "")
                for k in fields
            })
    print(f"Saved CSV to {path}")

def enrich_with_catalog(mappings: List[Dict[str, Any]], catalog: Dict[str, Dict[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    for m in mappings:
        if not m.get("transformation_logic"):
            m["transformation_logic"] = "Direct mapping"
        table = m["source_table"]
        column = m["source_column"]
        meta = catalog.get(table, {}).get(column, {})
        m["data_type"] = meta.get("data_type", "")
        m["is_categorical"] = meta.get("is_categorical", False)
        m["categories"] = meta.get("categories", [])
    return mappings

def extract_mappings_with_openai(etl_script: str, model: str = DEFAULT_MODEL) -> Dict[str, Any]:
    client = OpenAI()
    user_task = USER_TASK_TEMPLATE.format(etl_script=etl_script)
    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_INSTRUCTIONS},
            {"role": "user", "content": user_task},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "etl_mappings",
                "strict": True,
                "schema": SCHEMA,
            },
        },
        temperature=0,
    )
    raw_text = completion.choices[0].message.content
    data = json.loads(raw_text)
    if not isinstance(data, dict) or "mappings" not in data:
        raise ValueError("Response JSON missing 'mappings' list.")
    return data

def main():
    parser = argparse.ArgumentParser(description="Extract and enrich ETL mappings using OpenAI.")
    parser.add_argument("--etl", required=True, help="Path to ETL script file")
    parser.add_argument("--json", default="etl_mapping.json", help="Output JSON path")
    parser.add_argument("--csv", default="etl_mapping.csv", help="Output CSV path")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="OpenAI model to use")
    args = parser.parse_args()

    print("Analyzing ETL script with OpenAI...")
    etl_script = read_text(args.etl)
    catalog = json.load(open(CATALOG_PATH, "r", encoding="utf-8"))
    data = extract_mappings_with_openai(etl_script, model=args.model)
    enriched = enrich_with_catalog(data["mappings"], catalog)
    save_json({"mappings": enriched}, args.json)
    save_csv(enriched, args.csv)
    print(f" Extraction and enrichment complete. Rows: {len(enriched)}")

if __name__ == "__main__":
    main()
