"""
DQ Agent - dq_rule_generator.py 

Workflow:
1. Input:
   - etl_mapping.csv : STM output with source/target/logic/datatype/categories

2. Processing:
   - Load STM mapping
   - LLM generates DQ rules per target column (2–3 each)
   - Flatten rules into rows

3. Output:
   - dq_rules_output.csv : Flattened DQ rules aligned with STM mapping
   - State object (can be passed into larger LangGraph workflow)
"""

# --- 1. Imports ---
import os
import re
import logging
import pandas as pd
from typing import TypedDict, List, Dict
from dotenv import load_dotenv
load_dotenv()  # this will read from .env automatically


from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END

# --- 2. Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- 3. Define State ---
class DQState(TypedDict):
    stm_mapping: List[Dict]
    dq_rules: List[Dict]

# --- 4. Initialize LLM ---
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise EnvironmentError("OPENAI_API_KEY environment variable is not set.")

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# --- 5. Workflow Nodes ---
def load_stm(state: DQState) -> DQState:
    """Load STM mapping from CSV into state."""
    input_csv = "dq_agent/stm_sample.csv"
    logging.info(f"Loading STM mapping from {input_csv}")
    df = pd.read_csv(input_csv)
    state["stm_mapping"] = df.to_dict(orient="records")
    return state

def generate_rules(state: DQState) -> DQState:
    """Generate DQ rules for each target column using LLM."""
    dq_rules = []
    for row in state["stm_mapping"]:
        prompt = f"""
        You are a data quality expert. Based on the following metadata, suggest 2–3 validation rules.
        Rules should be concise, actionable, and domain-agnostic.

        Source table: {row.get('source_table', '')}
        Source column: {row.get('source_column', '')}
        Target table: {row.get('target_table', '')}
        Target column: {row.get('target_column', '')}
        Transformation logic: {row.get('transformation_logic', '')}
        Data Type: {row.get('data_type', '')}
        Is Categorical: {row.get('is_categorical', '')}
        Categories: {row.get('categories', '')}

        Return only the rules, as a comma-separated list. No commentary.
        """
        try:
            resp = llm.invoke(prompt)
            raw_rules = resp.content.strip()
            # Split by either commas or newlines
            candidates = re.split(r"[\n]+", raw_rules)

            # Clean numbering like "1. " or "2) "
            rules_list = [
                re.sub(r"^\s*\d+[\.\)]\s*", "", r.strip())
                for r in candidates if r.strip()
            ]

            for r in rules_list:
                dq_rules.append({
                    "source_table": row.get("source_table", ""),
                    "source_column": row.get("source_column", ""),
                    "target_table": row.get("target_table", ""),
                    "target_column": row.get("target_column", ""),
                    "rule": r
                })
        except Exception as e:
            logging.error(f"Error generating rules for {row}: {e}")
            dq_rules.append({
                "source_table": row.get("source_table", ""),
                "source_column": row.get("source_column", ""),
                "target_table": row.get("target_table", ""),
                "target_column": row.get("target_column", ""),
                "rule": f"[ERROR] {e}"
            })

    state["dq_rules"] = dq_rules
    return state

def save_rules(state: DQState) -> DQState:
    """Save generated rules to CSV."""
    output_csv = "dq_agent/dq_rules_output.csv"
    df = pd.DataFrame(state["dq_rules"])
    df.to_csv(output_csv, index=False)
    logging.info(f"DQ rules saved to {output_csv}")
    return state

# --- 6. Build Graph ---
graph = StateGraph(DQState)
graph.add_node("load", load_stm)
graph.add_node("rules", generate_rules)
graph.add_node("save", save_rules)

graph.set_entry_point("load")
graph.add_edge("load", "rules")
graph.add_edge("rules", "save")
graph.add_edge("save", END)

app = graph.compile()

# --- 7. Entrypoint ---
if __name__ == "__main__":
    final_state = app.invoke({})
    logging.info("DQ Rule Generation Workflow Complete")
