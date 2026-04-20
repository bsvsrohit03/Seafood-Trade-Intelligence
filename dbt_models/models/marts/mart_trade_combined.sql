-- File: dbt_models/models/marts/mart_trade_combined.sql
--
-- WHAT THIS DOES:
--   Joins value + quantity + country names + commodity names into one wide table.
--   Also derives unit_price_usd_per_tonne — the most analytically valuable column.
--   This is the table your Power BI dashboard and Streamlit app will query.
--
-- Star schema pattern:
--   fact table (value + quantity joined) + dimension tables (country, commodity)

WITH value AS (
    SELECT * FROM {{ ref('stg_trade_value') }}
),

quantity AS (
    SELECT * FROM {{ ref('stg_trade_quantity') }}
),

-- Join value and quantity on the 4-column natural key
combined AS (
    SELECT
        v.trade_flow_code,
        v.country_un_code,
        v.commodity_fao_code,
        v.trade_year,
        v.data_quality_flag,
        v.data_quality_label,
        v.value_usd_thousands,
        v.value_usd,
        q.quantity_tonnes,

        -- Unit price: how much is one tonne of this product worth?
        -- SAFE_DIVIDE returns NULL instead of error when quantity = 0
        SAFE_DIVIDE(v.value_usd, q.quantity_tonnes) AS unit_price_usd_per_tonne

    FROM value v
    LEFT JOIN quantity q
        ON  v.trade_flow_code    = q.trade_flow_code
        AND v.country_un_code    = q.country_un_code
        AND v.commodity_fao_code = q.commodity_fao_code
        AND v.trade_year         = q.trade_year
),

-- Join with country reference to get readable country names
with_country AS (
    SELECT
        c.*,
        co.Name_En         AS country_name,
        co.ISO3_Code       AS country_iso3,
        co.Continent_Group AS continent
    FROM combined c
    LEFT JOIN {{ source('fao_trade_raw', 'ref_country') }} co
        ON c.country_un_code = co.UN_Code
),

-- Join with commodity reference to get readable product names
final AS (
    SELECT
        w.*,
        CASE
            WHEN w.trade_flow_code = 'E' THEN 'Export'
            WHEN w.trade_flow_code = 'I' THEN 'Import'
            WHEN w.trade_flow_code = 'R' THEN 'Reexport'
            ELSE w.trade_flow_code
        END                        AS trade_flow_label,
        cm.commodity_name_en       AS commodity_name,
        cm.isscaap_group           AS isscaap_group
    FROM with_country w
    LEFT JOIN {{ source('fao_trade_raw', 'ref_commodity') }} cm
        ON w.commodity_fao_code = cm.fao_code
)

SELECT * FROM final
