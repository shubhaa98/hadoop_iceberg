#!/usr/bin/env python3
"""
LLM-first Lineage Generator

1. Reads the entire ETL script (PySpark SQL pipeline).
2. Sends it to OpenAI asking for lineage JSON.
3. Renders the lineage in Tableau/dbt-style with Graphviz.
"""

import os, json, argparse, re
from graphviz import Digraph
from openai import OpenAI

# ---------------- LLM Extraction ----------------
def extract_lineage_from_script(script_path: str) -> dict:
    with open(script_path, "r", encoding="utf-8") as f:
        src = f.read()

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    prompt = f"""
You are a data lineage analyzer.

Read this ETL PySpark script and extract the full lineage.

Return STRICT JSON with this format:
{{
  "steps": [
    {{
      "name": "<step name or df var>",
      "output": "<dataset/view produced>",
      "columns": ["col1", "col2", ...],
      "sources": [
        {{
          "name": "<source dataset>",
          "columns": ["colA", "colB", ...]
        }}
      ]
    }}
  ]
}}

Rules:
- Output JSON only (no markdown, no commentary).
- Include *all* datasets produced and consumed (staging, union, nested, final).
- Columns must come from the SELECT clauses.
- Step names can be df variable names (df, df_union, etc.) or view names.
- Do not omit any step.
- Do not wrap response in ```json.
Here is the ETL script:

{src}
"""

    resp = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )

    text = resp.choices[0].message.content.strip()
    # Strip accidental code fences
    if text.startswith("```"):
        text = re.sub(r"^```(json)?", "", text.strip())
        text = text.strip().rstrip("```").strip()

    return json.loads(text)


# ---------------- Rendering ----------------
def make_tableau_node(dot, dataset, columns):
    """Render a dataset as a Tableau/dbt style box"""
    label = f"""<
    <TABLE BORDER="1" CELLBORDER="1" CELLSPACING="0" BGCOLOR="white">
    <TR><TD COLSPAN="2" BGCOLOR="#1f77b4"><FONT COLOR="white"><B>{dataset}</B></FONT></TD></TR>
    {''.join(f'<TR><TD ALIGN="LEFT">{col}</TD></TR>' for col in columns)}
    </TABLE>>"""
    dot.node(dataset, label=label, shape="plaintext")


def render_lineage(lineage: dict, out_file="lineage"):
    dot = Digraph("Lineage", filename=out_file, format="png")
    dot.attr(rankdir="LR", splines="ortho")

    rendered = set()

    for step in lineage["steps"]:
        output = step["output"]
        make_tableau_node(dot, output, step.get("columns", []))
        rendered.add(output)

        for src in step.get("sources", []):
            if src["name"] not in rendered:
                make_tableau_node(dot, src["name"], src.get("columns", []))
                rendered.add(src["name"])
            dot.edge(src["name"], output)

    outfile = dot.render(cleanup=True)
    print(f" Lineage map saved as {outfile}")


# ---------------- Main ----------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--script", required=True, help="Path to etl_script.py")
    ap.add_argument("--out", default="lineage")
    ap.add_argument("--json", default="lineage.json", help="Path to save lineage JSON")
    args = ap.parse_args()

    lineage = extract_lineage_from_script(args.script)
    with open(args.json, "w", encoding="utf-8") as f:
        json.dump(lineage, f, indent=2)
    print(f"Saved lineage JSON to {args.json}")
    
    render_lineage(lineage, args.out)
