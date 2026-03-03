-- stg_transactions.sql
-- Silver layer: clean transactions with business rules applied

{{
    config(
        materialized='incremental',
        unique_key='txn_id',
        incremental_strategy='merge',
        tags=['staging', 'incremental']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        txn_id,
        user_id,
        ROUND(amount, 4)                                AS amount,
        UPPER(COALESCE(currency, 'USD'))                AS currency,
        LOWER(TRIM(status))                             AS status,
        LOWER(TRIM(type))                               AS txn_type,
        created_at::TIMESTAMP_NTZ                       AS created_at,
        updated_at::TIMESTAMP_NTZ                       AS updated_at,
        DATE(created_at)                                AS txn_date,
        CASE
            WHEN status = 'completed' THEN TRUE
            ELSE FALSE
        END                                             AS is_completed,
        CASE
            WHEN amount > 1000 THEN 'high'
            WHEN amount > 100  THEN 'medium'
            ELSE 'low'
        END                                             AS amount_tier,
        _ingested_at
    FROM source
    WHERE txn_id IS NOT NULL
      AND amount > 0
      AND created_at IS NOT NULL
)

SELECT * FROM cleaned
