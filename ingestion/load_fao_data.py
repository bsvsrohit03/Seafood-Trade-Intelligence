# File: ingestion/load_fao_data.py
import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET = os.getenv("BQ_DATASET_RAW")

client = bigquery.Client(project=PROJECT)

# Actual FAO column names → our clean names
TRADE_COLUMN_MAP = {
    "TRADE_FLOW.ALPHA_CODE":    "TRADE_FLOW_CODE",
    "COUNTRY_REPORTER.UN_CODE": "COUNTRY_UN_CODE",
    "COMMODITY.FAO_CODE":       "COMMODITY_FAO_CODE",
    "MEASURE":                  "MEASURE",
    "PERIOD":                   "PERIOD",
    "STATUS":                   "STATUS",
    "VALUE":                    "VALUE",
}

TRADE_SCHEMA = [
    bigquery.SchemaField("TRADE_FLOW_CODE",    "STRING"),
    bigquery.SchemaField("COUNTRY_UN_CODE",    "STRING"),
    bigquery.SchemaField("COMMODITY_FAO_CODE", "STRING"),
    bigquery.SchemaField("MEASURE",            "STRING"),
    bigquery.SchemaField("PERIOD",             "STRING"),
    bigquery.SchemaField("STATUS",             "STRING"),
    bigquery.SchemaField("VALUE",              "STRING"),
]


def load_trade_csv(filepath, table_name):
    print(f"\nLoading {filepath} into {DATASET}.{table_name}...")

    df = pd.read_csv(filepath, dtype=str, encoding="utf-8", on_bad_lines="skip")
    print(f"  Raw columns: {list(df.columns)}")
    print(f"  Rows: {len(df):,}")

    # Rename actual FAO columns to our clean names
    df = df.rename(columns=TRADE_COLUMN_MAP)

    # Keep only the 7 columns we need
    df = df[["TRADE_FLOW_CODE", "COUNTRY_UN_CODE", "COMMODITY_FAO_CODE",
             "MEASURE", "PERIOD", "STATUS", "VALUE"]]

    table_ref  = f"{PROJECT}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=TRADE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"  OK BigQuery confirms: {table.num_rows:,} rows in {table_name}")


def main():

    # TRADE VALUE
    load_trade_csv("data/raw/TRADE_VALUE.csv", "raw_trade_value")

    # TRADE QUANTITY
    load_trade_csv("data/raw/TRADE_QUANTITY.csv", "raw_trade_quantity")

    # COMMODITY LOOKUP
    print("\nLoading commodity lookup...")
    df_c = pd.read_csv("data/raw/CL_FI_COMMODITY_ISSCFC.csv", dtype=str,
                       encoding="utf-8", on_bad_lines="skip")
    print(f"  Commodity columns: {list(df_c.columns)}")

    code_col = next((c for c in df_c.columns if "Code" in c or "code" in c), df_c.columns[0])
    name_col = next((c for c in df_c.columns if "Name_En" in c or "name" in c.lower()), df_c.columns[1])
    grp_col  = next((c for c in df_c.columns if "ISSCAAP" in c or "Group" in c), df_c.columns[2])

    df_commodity = df_c[[code_col, name_col, grp_col]].copy()
    df_commodity.columns = ["fao_code", "commodity_name_en", "isscaap_group"]

    job_config_simple = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = client.load_table_from_dataframe(
        df_commodity, f"{PROJECT}.{DATASET}.ref_commodity", job_config=job_config_simple
    )
    job.result()
    print(f"  OK Loaded {len(df_commodity):,} commodity codes")

    # COUNTRY LOOKUP
    print("\nLoading country lookup...")
    df_co = pd.read_csv("data/raw/CL_FI_COUNTRY_GROUPS.csv", dtype=str,
                        encoding="utf-8", on_bad_lines="skip")
    print(f"  Country columns: {list(df_co.columns)}")

    keep = [c for c in ["UN_Code", "Name_En", "ISO2_Code", "ISO3_Code", "Continent_Group"]
            if c in df_co.columns]
    df_country = df_co[keep].copy()

    job = client.load_table_from_dataframe(
        df_country, f"{PROJECT}.{DATASET}.ref_country", job_config=job_config_simple
    )
    job.result()
    print(f"  OK Loaded {len(df_country):,} countries")

    print("\nAll 4 tables loaded successfully. Check your BigQuery console to verify.")


if __name__ == "__main__":
    main()
