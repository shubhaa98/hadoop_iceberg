import streamlit as st
import pandas as pd
import json

# Load STM JSON
with open("etl_mapping.json", "r") as f:
    stm_data = json.load(f)

# Convert STM mappings to DataFrasme
stm_df = pd.DataFrame(stm_data["mappings"])

# Page title
st.title("Hadoop → Iceberg Migration Accelerators")

st.header("Source-to-Target Mapping (STM)")
st.write("This table shows how source fields are mapped into Iceberg target tables.")
st.dataframe(stm_df)

st.header("Data Lineage Map")
st.write("This diagram shows the end-to-end data flow into the Iceberg tables.")
st.image("lineage.svg", use_column_width=True)
