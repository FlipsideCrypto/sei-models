{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'fact_prices_ohlc_hourly_id',
    cluster_by = ['HOUR::DATE','asset_id'],
    tags = ['core']
) }}

SELECT
    asset_id,
    recorded_hour AS HOUR,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_prices_id AS fact_prices_ohlc_hourly_id
FROM
    {{ ref('silver__complete_provider_prices') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
