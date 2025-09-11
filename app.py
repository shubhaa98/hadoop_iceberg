import streamlit as st
import pandas as pd
import tempfile
import os
import subprocess
from io import StringIO

# ==============================
# Page / App Configuration
# ==============================
st.set_page_config(page_title="ETL Lineage Mapper", layout="wide")
st.title("ETL Lineage Mapping and Visualization")

# Filenames your agents write. Keep these as-is unless your agents differ.
ETL_MAPPING_CSV = "etl_mapping.csv"   # produced by STM agent
LINEAGE_SVG = "lineage.svg"           # produced by Lineage agent

# ==============================
# Session State (kept on reruns)
# ==============================
defaults = {
    "etl_path": "",            # absolute path of the uploaded script in temp
    "stm_df": None,            # STM dataframe kept in memory (prevents disappearing)
    "stm_stdout": "",          # raw STDOUT/STDERR from STM
    "stm_ready": False,        # True after STM doc successfully loaded
    "lineage_stdout": "",      # raw STDOUT/STDERR from lineage
    "lineage_ready": False,    # True after lineage completes successfully
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ==============================
# Helpers
# ==============================
def run_cmd(cmd):
    """Run a subprocess command and return (ok, stdout, stderr)."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True, result.stdout, ""
    except subprocess.CalledProcessError as e:
        return False, "", (e.stderr or e.stdout or "")

def save_uploaded_file(uploaded_file) -> str:
    """Save uploaded file to a temp path and return the absolute path."""
    temp_dir = tempfile.gettempdir()
    temp_path = os.path.join(temp_dir, uploaded_file.name)
    with open(temp_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return temp_path

# ==============================
# 1) Upload ETL Script
# ==============================
st.header("1. Upload ETL Script")

uploaded = st.file_uploader("Select a .py, .sql, or .txt file", type=["py", "sql", "txt"])
if uploaded is not None:
    st.session_state.etl_path = save_uploaded_file(uploaded)
    st.write(f"Uploaded: {uploaded.name}")

    # Reset downstream state on new upload
    st.session_state.stm_df = None
    st.session_state.stm_stdout = ""
    st.session_state.stm_ready = False
    st.session_state.lineage_stdout = ""
    st.session_state.lineage_ready = False

# ==============================
# 2) STM Documentation
# ==============================
st.header("2. STM Documentation")

# Run STM
run_stm_disabled = not bool(st.session_state.etl_path)
if st.button("Run STM Agent", key="run_stm", disabled=run_stm_disabled):
    ok, out, err = run_cmd(["python", "stm_run_openai.py", "--etl", st.session_state.etl_path])
    st.session_state.stm_stdout = out if ok else err

    # On success, read the CSV ONCE and persist in memory so it won't disappear
    if ok and os.path.exists(ETL_MAPPING_CSV):
        try:
            st.session_state.stm_df = pd.read_csv(ETL_MAPPING_CSV)
            st.session_state.stm_ready = True
        except Exception as read_err:
            st.session_state.stm_df = None
            st.session_state.stm_ready = False
            st.session_state.stm_stdout += f"\nFailed to read {ETL_MAPPING_CSV}: {read_err}"
    else:
        # Keep previous STM on screen if it existed; just report the issue
        if ok:
            st.session_state.stm_stdout += f"\nExpected file not found: {ETL_MAPPING_CSV}"
        # If we had nothing before, keep stm_ready=False

# Always render STM from memory (not gated by file existence)
if st.session_state.stm_ready and st.session_state.stm_df is not None:
    st.dataframe(st.session_state.stm_df, use_container_width=True)

    # Optional: simple CSV download (avoids Excel engine dependency)
    csv_buffer = StringIO()
    st.session_state.stm_df.to_csv(csv_buffer, index=False)
    st.download_button(
        "Download STM (CSV)",
        data=csv_buffer.getvalue(),
        file_name="etl_mapping.csv",
        mime="text/csv",
        key="dl_stm_csv",
    )
else:
    st.write("STM table will appear here after you run the STM agent.")

# Plain-text logs (no icons/spinners)
if st.session_state.stm_stdout:
    st.text_area("STM output", st.session_state.stm_stdout, height=160)

# ==============================
# 3) Data Lineage (always below STM)
# ==============================
st.header("3. Data Lineage")

# Trigger lineage under STM so the lineage UI always appears below
run_lineage_disabled = not bool(st.session_state.etl_path)
if st.button("Run Data Lineage Agent", key="run_lineage", disabled=run_lineage_disabled):
    # IMPORTANT: pass --script <temp_etl_path> as requested
    ok, out, err = run_cmd(["python", "lineage_mapping.py", "--script", st.session_state.etl_path])
    st.session_state.lineage_stdout = out if ok else err
    st.session_state.lineage_ready = ok

# Render lineage graph (does NOT affect STM visibility)
if st.session_state.lineage_ready:
    if os.path.exists(LINEAGE_SVG):
        st.image(LINEAGE_SVG, caption="Lineage Graph", width=900)
        with open(LINEAGE_SVG, "rb") as f:
            st.download_button("Download Lineage (SVG)", f, file_name="lineage_graph.svg", key="dl_lineage_svg")
    else:
        st.write(f"Expected file not found: {LINEAGE_SVG}")
else:
    st.write("Lineage graph will appear here after you run the lineage agent.")

if st.session_state.lineage_stdout:
    st.text_area("Lineage output", st.session_state.lineage_stdout, height=160)
