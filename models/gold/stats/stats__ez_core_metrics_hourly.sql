{{ config(
    materialized = 'view',
    tags = ['noncore','recent_test'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    }} }
) }}

WITH txs AS (

    SELECT
        block_timestamp_hour,
        transaction_count,
        transaction_count_success,
        transaction_count_failed,
        unique_from_count,
        total_fees AS total_fees_native,
        LAST_VALUE(
            p.price ignore nulls
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
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON s.block_timestamp_hour = p.hour
        AND p.token_address = 'usei'
)
SELECT
    A.block_timestamp_hour,
    A.block_number_min,
    A.block_number_max,
    A.block_count,
    b.transaction_count,
    b.transaction_count_success,
    b.transaction_count_failed,
    b.unique_from_count,
    b.total_fees_native,
    ROUND(
        b.total_fees_native * b.imputed_close,
        2
    ) AS total_fees_usd,
    A.core_metrics_block_hourly_id AS ez_core_metrics_hourly_id,
    GREATEST(
        A.inserted_timestamp,
        b.inserted_timestamp
    ) AS inserted_timestamp,
    GREATEST(
        A.modified_timestamp,
        b.modified_timestamp
    ) AS modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_block_hourly') }} A
    JOIN txs b
    ON A.block_timestamp_hour = b.block_timestamp_hour
