-- dim_users.sql
-- Gold layer: enriched user dimension

{{
    config(
        materialized='table',
        tags=['marts', 'users']
    )
}}

WITH users AS (
    SELECT * FROM {{ source('raw', 'users') }}
),

user_txn_stats AS (
    SELECT
        user_id,
        COUNT(*)                AS total_transactions,
        SUM(amount)             AS lifetime_value,
        MIN(created_at)         AS first_transaction_at,
        MAX(created_at)         AS last_transaction_at,
        AVG(amount)             AS avg_order_value,
        DATEDIFF('day',
            MIN(created_at),
            MAX(created_at))    AS tenure_days
    FROM {{ ref('stg_transactions') }}
    WHERE is_completed = TRUE
    GROUP BY 1
),

enriched AS (
    SELECT
        u.user_id,
        u.email,
        u.created_at                                    AS signup_date,
        t.total_transactions,
        ROUND(t.lifetime_value, 2)                      AS lifetime_value,
        t.first_transaction_at,
        t.last_transaction_at,
        t.tenure_days,
        ROUND(t.avg_order_value, 2)                     AS avg_order_value,
        CASE
            WHEN t.lifetime_value >= 10000 THEN 'platinum'
            WHEN t.lifetime_value >= 1000  THEN 'gold'
            WHEN t.lifetime_value >= 100   THEN 'silver'
            ELSE 'bronze'
        END                                             AS user_tier,
        DATEDIFF('day', t.last_transaction_at,
            CURRENT_DATE())                             AS days_since_last_txn,
        CURRENT_TIMESTAMP()                             AS _updated_at
    FROM users u
    LEFT JOIN user_txn_stats t USING (user_id)
)

SELECT * FROM enriched
