-- stg_events.sql
-- Silver layer: clean and type-cast raw events

{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        cluster_by=['event_date'],
        tags=['staging', 'incremental']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'events') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

renamed AS (
    SELECT
        event_id,
        user_id,
        LOWER(TRIM(event_type))                         AS event_type,
        session_id,
        properties,
        received_at::TIMESTAMP_NTZ                      AS received_at,
        DATE(received_at)                               AS event_date,
        EXTRACT(HOUR FROM received_at)                  AS event_hour,
        properties:page::VARCHAR                        AS page,
        properties:duration_ms::NUMBER                  AS duration_ms,
        properties:referrer::VARCHAR                    AS referrer,
        _ingested_at,
        _source_file,
        CURRENT_TIMESTAMP()                             AS _transformed_at
    FROM source
    WHERE event_id IS NOT NULL
      AND user_id IS NOT NULL
      AND received_at IS NOT NULL
)

SELECT * FROM renamed
