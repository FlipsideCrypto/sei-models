{{ config(
    materialized = 'incremental',
    unique_key = ['id','recorded_hour'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'recorded_hour::DATE',
    tags = ['noncore']
) }}

SELECT
    id,
    recorded_hour,
    OPEN,
    high,
    low,
    CLOSE,
    _INSERTED_TIMESTAMP,
    hourly_prices_coin_gecko_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'hourly_prices_coin_gecko'
    ) }}
WHERE
    id IN (
        SELECT
            DISTINCT coingecko
        FROM
            {{ ref('silver__prices_api') }}
        WHERE
            COALESCE(
                coingecko,
                ''
            ) <> ''
    )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
