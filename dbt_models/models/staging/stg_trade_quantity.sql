-- File: dbt_models/models/staging/stg_trade_quantity.sql
--
-- Same pattern as stg_trade_value but for physical volume (metric tonnes).

SELECT
    TRADE_FLOW_CODE                AS trade_flow_code,
    COUNTRY_UN_CODE                AS country_un_code,
    COMMODITY_FAO_CODE             AS commodity_fao_code,
    CAST(PERIOD AS INT64)          AS trade_year,
    STATUS                         AS data_quality_flag,
    CAST(VALUE AS FLOAT64)         AS quantity_tonnes

FROM {{ source('fao_trade_raw', 'raw_trade_quantity') }}

WHERE STATUS != 'N'
  AND VALUE IS NOT NULL
  AND CAST(VALUE AS FLOAT64) > 0
