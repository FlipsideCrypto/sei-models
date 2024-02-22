{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['noncore']
) }}

SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS block_timestamp_hour,
    COUNT(
        DISTINCT tx_id
    ) AS transaction_count,
    COUNT(
        DISTINCT CASE
            WHEN tx_succeeded THEN tx_id
        END
    ) AS transaction_count_success,
    COUNT(
        DISTINCT CASE
            WHEN NOT tx_succeeded THEN tx_id
        END
    ) AS transaction_count_failed,
    COUNT(
        DISTINCT tx_from
    ) AS unique_from_count,
    SUM(
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    fee,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) / pow(
            10,
            6
        )
    ) AS total_fees,
    MAX(_inserted_timestamp) AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transactions_final') }}
WHERE
    block_timestamp_hour < DATE_TRUNC('hour', systimestamp())

{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    _inserted_timestamp
) >= (
    SELECT
        MAX(DATE_TRUNC('hour', _inserted_timestamp)) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1
