# File: app/main.py
#
# WHAT THIS DOES:
#   A Streamlit web app where users ask plain English questions about seafood trade.
#   The app sends the question + schema context to Claude, which writes BigQuery SQL.
#   The query runs on BigQuery and results are displayed back to the user.
#
# RUN WITH: streamlit run app/main.py

import os
import json
import tempfile
import streamlit as st
import pandas as pd
from google.cloud import bigquery
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()
# ── INITIALISE CLIENTS ────────────────────────────────────────────────────────
# Write GCP credentials from Streamlit secrets to a temp file
if "gcp_service_account" in st.secrets:
    creds_dict = dict(st.secrets["gcp_service_account"])
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(creds_dict, f)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name

bq_client = bigquery.Client(project=os.getenv("GOOGLE_CLOUD_PROJECT"))
anthropic_key = st.secrets.get("ANTHROPIC_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
claude_client = Anthropic(api_key=anthropic_key)
PROJECT = st.secrets.get("GOOGLE_CLOUD_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")

# ── DATABASE SCHEMA CONTEXT ───────────────────────────────────────────────────
# Sent to Claude so it knows the structure of your database.
# The more detailed this is, the better the SQL it writes.
SCHEMA_CONTEXT = f"""
You are a SQL expert for a seafood trade analytics database on BigQuery.

The main table is: `{PROJECT}.fao_trade_marts_marts.mart_trade_combined`

COLUMNS:
  trade_year                 INTEGER   Year of the trade (1976 to 2023)
  trade_flow_code            STRING    'E' = Export, 'I' = Import, 'R' = Reexport
  trade_flow_label           STRING    'Export', 'Import', 'Reexport' (human-readable)
  country_name               STRING    Name of the reporting country (e.g. 'India', 'Norway')
  country_iso3               STRING    ISO3 country code (e.g. 'IND', 'NOR')
  commodity_name             STRING    Product name (e.g. 'Shrimps, frozen', 'Salmon, fresh')
  commodity_fao_code         STRING    FAO ISSCFC commodity code
  isscaap_group              STRING    Broad species group number
  value_usd                  FLOAT64   Trade value in full USD
  value_usd_thousands        FLOAT64   Trade value in USD thousands
  quantity_tonnes            FLOAT64   Volume in metric tonnes (product weight)
  unit_price_usd_per_tonne   FLOAT64   Derived: value_usd / quantity_tonnes
  data_quality_flag          STRING    'A' = Official, 'E' = Estimated
  data_quality_label         STRING    'Official', 'Estimated'

RULES:
1. Always use the full table path: `{PROJECT}.fao_trade_marts_marts.mart_trade_combined`
2. Always include a LIMIT clause (default 100, max 500)
3. Only use data_quality_flag IN ('A', 'E') unless the user asks otherwise
4. For aggregations, use SUM(value_usd) for value and SUM(quantity_tonnes) for volume
5. Return ONLY the SQL query — no explanation, no markdown backticks, no comments
"""


def ask_claude_for_sql(user_question: str) -> str:
    """
    Send the user's question to Claude and get back a BigQuery SQL query.
    Claude knows the schema from SCHEMA_CONTEXT above.
    """
    response = claude_client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        system=SCHEMA_CONTEXT,
        messages=[
            {
                "role": "user",
                "content": f"Write a BigQuery SQL query to answer: {user_question}",
            }
        ],
    )
    return response.content[0].text.strip()


def run_bigquery_sql(sql: str) -> pd.DataFrame:
    """
    Execute a SQL query on BigQuery and return results as a pandas DataFrame.
    """
    query_job = bq_client.query(sql)
    return query_job.to_dataframe()


# ── STREAMLIT UI ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Seafood Trade Intelligence",
    page_icon="🐟",
    layout="wide",
)

st.title("🐟 Seafood Trade Intelligence Dashboard")
st.markdown(
    "Ask any question about global seafood trade **(1976–2023)**. "
    "Powered by FAO FishStat data + Claude AI."
)

# ── SIDEBAR: example questions ────────────────────────────────────────────────
st.sidebar.header("💡 Example questions")
examples = [
    "Which 5 countries exported the most shrimp by value in 2022?",
    "How has the global price per tonne of salmon changed since 2000?",
    "Which continent imported the most seafood in 2020?",
    "Show me the top 10 commodities by total export value in 2023",
    "Which countries had the biggest increase in tuna exports between 2010 and 2022?",
    "What is India's top exported seafood commodity by volume?",
    "Compare Norway and Chile salmon export values from 2010 to 2023",
]

for ex in examples:
    if st.sidebar.button(ex, use_container_width=True):
        st.session_state["question"] = ex

# ── MAIN INPUT ────────────────────────────────────────────────────────────────
question = st.text_input(
    "Your question:",
    value=st.session_state.get("question", ""),
    placeholder="e.g. Which country exported the most shrimp in 2022?",
)

if st.button("Ask", type="primary") and question:

    with st.spinner("🤖 Generating SQL query with Claude..."):
        sql = ask_claude_for_sql(question)

    # Show the generated SQL (useful for learning and debugging)
    with st.expander("🔍 Generated SQL", expanded=False):
        st.code(sql, language="sql")

    with st.spinner("⚡ Running query on BigQuery..."):
        try:
            df = run_bigquery_sql(sql)
            st.success(f"✅ Query returned **{len(df):,} rows**")
            st.dataframe(df, use_container_width=True)

            # Offer a quick bar chart if results have numeric + text columns
            numeric_cols = df.select_dtypes(include="number").columns.tolist()
            text_cols    = df.select_dtypes(include="object").columns.tolist()

            if numeric_cols and text_cols:
                st.subheader("📊 Quick visualisation")
                col1, col2 = st.columns(2)
                with col1:
                    x_col = st.selectbox("X axis (category)", text_cols,    index=0)
                with col2:
                    y_col = st.selectbox("Y axis (value)",    numeric_cols, index=0)
                chart_df = df[[x_col, y_col]].set_index(x_col).head(20)
                st.bar_chart(chart_df)

        except Exception as e:
            st.error(f"❌ Query failed: {e}")
            st.info("Try rephrasing your question, or inspect the SQL above for errors.")

# ── FOOTER ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Data source: FAO Global Aquatic Trade Statistics (FishStat) — CC BY 4.0 | "
    "fao.org/fishery/statistics-query/en/trade"
)
