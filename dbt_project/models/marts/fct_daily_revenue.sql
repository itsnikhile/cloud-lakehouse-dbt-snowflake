-- fct_daily_revenue.sql
-- Gold layer: daily revenue metrics for BI consumption

{{
    config(
        materialized='table',
        tags=['marts', 'finance'],
        post_hook="ALTER TABLE {{ this }} CLUSTER BY (txn_date)"
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    WHERE is_completed = TRUE
),

daily_metrics AS (
    SELECT
        txn_date,
        currency,
        COUNT(*)                                        AS txn_count,
        COUNT(DISTINCT user_id)                         AS unique_customers,
        SUM(amount)                                     AS gross_revenue,
        AVG(amount)                                     AS avg_order_value,
        MEDIAN(amount)                                  AS median_order_value,
        PERCENTILE_CONT(0.95) WITHIN GROUP
            (ORDER BY amount)                           AS p95_order_value,
        SUM(CASE WHEN amount_tier = 'high'
            THEN amount ELSE 0 END)                     AS high_value_revenue,
        SUM(CASE WHEN amount_tier = 'high'
            THEN 1 ELSE 0 END)                          AS high_value_count
    FROM transactions
    GROUP BY 1, 2
)

SELECT
    *,
    SUM(gross_revenue) OVER (
        PARTITION BY currency ORDER BY txn_date
        ROWS UNBOUNDED PRECEDING
    )                                                   AS cumulative_revenue,
    gross_revenue / LAG(gross_revenue) OVER
        (PARTITION BY currency ORDER BY txn_date) - 1  AS revenue_growth_rate,
    CURRENT_TIMESTAMP()                                 AS _updated_at
FROM daily_metrics
ORDER BY txn_date DESC
