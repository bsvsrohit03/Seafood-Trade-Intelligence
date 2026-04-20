-- File: dbt_models/models/marts/mart_top_exporters.sql
--
-- WHAT THIS DOES:
--   Aggregates to show top exporting countries by commodity and decade.
--   Used for the "market leaders" view in the Power BI dashboard.

SELECT
    country_name,
    country_iso3,
    continent,
    commodity_name,
    isscaap_group,
    trade_year,
    FLOOR(trade_year / 10) * 10          AS decade,   -- 1980, 1990, 2000, 2010, 2020
    SUM(value_usd)                        AS total_export_value_usd,
    SUM(quantity_tonnes)                  AS total_export_tonnes,
    AVG(unit_price_usd_per_tonne)         AS avg_unit_price_usd_per_tonne,
    COUNT(*)                              AS record_count

FROM {{ ref('mart_trade_combined') }}

WHERE trade_flow_code  = 'E'             -- exports only
  AND country_name     IS NOT NULL
  AND commodity_name   IS NOT NULL
  AND data_quality_flag IN ('A', 'E')    -- official + estimated, no near-zero

GROUP BY 1, 2, 3, 4, 5, 6, 7
