# File: ingestion/load_fao_data.py
#
# WHAT THIS DOES:
# Reads the five FAO CSV files from data/raw/, cleans them up,
# and loads them into BigQuery as separate tables.
#
# RUN WITH: python ingestion/load_fao_data.py

import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET = os.getenv("BQ_DATASET_RAW")

# Initialise the BigQuery client
# Uses credentials.json automatically via GOOGLE_APPLICATION_CREDENTIALS
client = bigquery.Client(project=PROJECT)


def load_csv_to_bigquery(filepath: str, table_name: str, schema: list):
    """
    Generic function to load a CSV into a BigQuery table.

    Args:
        filepath:   path to the CSV file
        table_name: name of the BigQuery table to create/replace
        schema:     list of BigQuery SchemaField objects defining column types
    """
    print(f"\nLoading {filepath} into {DATASET}.{table_name}...")

    # Read everything as string first to avoid type-inference errors
    df = pd.read_csv(filepath, dtype=str)
    print(f"  Rows loaded: {len(df):,} | Columns: {list(df.columns)}")

    table_ref = f"{PROJECT}.{DATASET}.{table_name}"

    # WRITE_TRUNCATE = replace the table if it already exists
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,          # skip header row
        allow_quoted_newlines=True,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # block until complete

    table = client.get_table(table_ref)
    print(f"  ✅ BigQuery confirms: {table.num_rows:,} rows in {table_name}")


def main():

    # ── TRADE VALUE TABLE ─────────────────────────────────────────────────────
    # Monetary values (USD thousands) for each trade transaction.
    # One row = one country × one commodity × one year × one trade direction.
    load_csv_to_bigquery(
        filepath="data/raw/TRADE_VALUE.csv",
        table_name="raw_trade_value",
        schema=[
            bigquery.SchemaField("TRADE_FLOW_CODE",   "STRING",  description="E=Export, I=Import, R=Reexport"),
            bigquery.SchemaField("COUNTRY_UN_CODE",   "STRING",  description="UN numeric country code"),
            bigquery.SchemaField("COMMODITY_FAO_CODE","STRING",  description="ISSCFC commodity code"),
            bigquery.SchemaField("MEASURE",           "STRING",  description="Always V_USD_1000"),
            bigquery.SchemaField("PERIOD",            "INTEGER", description="Year e.g. 2005"),
            bigquery.SchemaField("STATUS",            "STRING",  description="A=Official, E=Estimated, N=Near-zero"),
            bigquery.SchemaField("VALUE",             "FLOAT64", description="Value in USD thousands"),
        ],
    )

    # ── TRADE QUANTITY TABLE ──────────────────────────────────────────────────
    # Same structure but VALUE contains tonnes (product weight) instead of USD.
    load_csv_to_bigquery(
        filepath="data/raw/TRADE_QUANTITY.csv",
        table_name="raw_trade_quantity",
        schema=[
            bigquery.SchemaField("TRADE_FLOW_CODE",   "STRING"),
            bigquery.SchemaField("COUNTRY_UN_CODE",   "STRING"),
            bigquery.SchemaField("COMMODITY_FAO_CODE","STRING"),
            bigquery.SchemaField("MEASURE",           "STRING"),
            bigquery.SchemaField("PERIOD",            "INTEGER"),
            bigquery.SchemaField("STATUS",            "STRING"),
            bigquery.SchemaField("VALUE",             "FLOAT64"),
        ],
    )

    # ── COMMODITY LOOKUP TABLE ────────────────────────────────────────────────
    # Maps FAO codes → human-readable names (e.g. "034.1.1.1.11" → "Shrimps, frozen")
    print("\nLoading commodity lookup table...")
    df_commodity = pd.read_csv("data/raw/CL_FI_COMMODITY_ISSCFC.csv", dtype=str)
    df_commodity_clean = df_commodity[["Code", "Name_En", "ISSCAAP"]].rename(columns={
        "Code":     "fao_code",
        "Name_En":  "commodity_name_en",
        "ISSCAAP":  "isscaap_group",
    })
    table_ref = f"{PROJECT}.{DATASET}.ref_commodity"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = client.load_table_from_dataframe(df_commodity_clean, table_ref, job_config=job_config)
    job.result()
    print(f"  ✅ Loaded commodity lookup: {len(df_commodity_clean):,} commodity codes")

    # ── COUNTRY LOOKUP TABLE ──────────────────────────────────────────────────
    # Maps UN numeric codes → country names (e.g. "356" → "India")
    print("\nLoading country lookup table...")
    df_country = pd.read_csv("data/raw/CL_FI_COUNTRY_GROUPS.csv", dtype=str)
    print(f"  Country file columns: {list(df_country.columns)}")

    cols_to_keep = [
        c for c in ["UN_Code", "Name_En", "ISO2_Code", "ISO3_Code", "Continent_Group"]
        if c in df_country.columns
    ]
    df_country_clean = df_country[cols_to_keep]

    table_ref = f"{PROJECT}.{DATASET}.ref_country"
    job = client.load_table_from_dataframe(df_country_clean, table_ref, job_config=job_config)
    job.result()
    print(f"  ✅ Loaded country lookup: {len(df_country_clean):,} countries")

    print("\n✅ All tables loaded successfully. Check your BigQuery console to verify.")


if __name__ == "__main__":
    main()
