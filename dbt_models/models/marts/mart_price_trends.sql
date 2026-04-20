-- File: dbt_models/models/marts/mart_price_trends.sql
--
-- WHAT THIS DOES:
--   Tracks unit price per tonne by commodity over time.
--   This is the price volatility analysis — one of the most powerful
--   insights for seafood supply chain decision-makers.

SELECT
    trade_year,
    commodity_name,
    isscaap_group,
    trade_flow_label,

    -- Global weighted average price for this commodity in this year
    SAFE_DIVIDE(
        SUM(value_usd),
        SUM(quantity_tonnes)
    )                              AS global_avg_price_usd_per_tonne,

    SUM(value_usd)                 AS global_total_value_usd,
    SUM(quantity_tonnes)           AS global_total_tonnes,
    COUNT(DISTINCT country_un_code) AS reporting_country_count

FROM {{ ref('mart_trade_combined') }}

WHERE commodity_name  IS NOT NULL
  AND quantity_tonnes > 0
  AND data_quality_flag IN ('A', 'E')

GROUP BY 1, 2, 3, 4
ORDER BY trade_year, commodity_name
