{{ config(
    materialized = 'view',
    tags = ['noncore'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    }} }
) }}

WITH base AS (

    SELECT
        block_timestamp_hour,
        block_number_min,
        block_number_max,
        block_count,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count,
        total_fees AS total_fees_native,
        LAST_VALUE(
            p.close ignore nulls
        ) over (
            ORDER BY
                block_timestamp_hour rows unbounded preceding
        ) AS imputed_close,
        core_metrics_hourly_id AS ez_core_metrics_hourly_id,
        s.inserted_timestamp AS inserted_timestamp,
        s.modified_timestamp AS modified_timestamp
    FROM
        {{ ref('silver_stats__core_metrics_hourly') }}
        s
        LEFT JOIN {{ ref('silver__hourly_prices_coin_gecko') }}
        p
        ON s.block_timestamp_hour = p.recorded_hour
        AND p.id = 'sei-network'
)
SELECT
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_from_count,
    total_fees_native,
    ROUND(
        total_fees_native * imputed_close,
        2
    ) AS total_fees_usd,
    ez_core_metrics_hourly_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base
