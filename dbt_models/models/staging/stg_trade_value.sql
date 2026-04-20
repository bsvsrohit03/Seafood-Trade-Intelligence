-- File: dbt_models/models/staging/stg_trade_value.sql
--
-- WHAT THIS DOES:
--   1. Renames raw column names to readable ones
--   2. Casts PERIOD (year) to INT64
--   3. Filters out rows where STATUS = 'N' (near-zero, not meaningful)
--   4. Converts value from USD thousands to full USD
--
-- STATUS codes:
--   A = Official reported data   ← most reliable
--   E = Estimated by FAO         ← use but flag
--   N = Not significant (<0.5)   ← drop
--   X = Special cases            ← keep, investigate later

SELECT
    TRADE_FLOW_CODE                       AS trade_flow_code,
    COUNTRY_UN_CODE                       AS country_un_code,
    COMMODITY_FAO_CODE                    AS commodity_fao_code,
    CAST(PERIOD AS INT64)                 AS trade_year,
    STATUS                                AS data_quality_flag,
    CAST(VALUE AS FLOAT64)                AS value_usd_thousands,
    CAST(VALUE AS FLOAT64) * 1000         AS value_usd,    -- full dollar value

    CASE
        WHEN STATUS = 'A' THEN 'Official'
        WHEN STATUS = 'E' THEN 'Estimated'
        WHEN STATUS = 'N' THEN 'Near-zero'
        ELSE 'Other'
    END                                   AS data_quality_label

FROM {{ source('fao_trade_raw', 'raw_trade_value') }}

WHERE STATUS != 'N'
  AND VALUE IS NOT NULL
  AND CAST(VALUE AS FLOAT64) > 0
