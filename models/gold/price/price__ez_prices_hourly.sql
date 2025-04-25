{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'ez_prices_hourly_id',
    cluster_by = ['hour::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(token_address, symbol, name)",
    tags = ['core']
) }}

WITH base AS (

    SELECT
        HOUR,
        token_address,
        price,
        blockchain,
        FALSE AS is_native,
        is_imputed,
        is_deprecated,
        inserted_timestamp,
        modified_timestamp,
        complete_token_prices_id AS ez_prices_hourly_id
    FROM
        {{ ref('silver__complete_token_prices') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                DATEADD(
                    'minute',
                    -90,
                    modified_timestamp
                )
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    HOUR,
    'usei' AS token_address,
    price,
    blockchain,
    TRUE AS is_native,
    is_imputed,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_native_prices_id AS ez_prices_hourly_id
FROM
    {{ ref('silver__complete_native_prices') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                DATEADD(
                    'minute',
                    -90,
                    modified_timestamp
                )
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    HOUR,
    A.token_address,
    b.symbol,
    b.name,
    b.decimals,
    price,
    A.blockchain,
    A.is_native,
    A.is_imputed,
    A.is_deprecated,
    A.inserted_timestamp,
    A.modified_timestamp,
    A.ez_prices_hourly_id
FROM
    base A
    LEFT JOIN {{ ref('price__ez_asset_metadata') }}
    b
    ON A.token_address = b.token_address

qualify row_number() over (partition by a.token_address, a.hour order by a.blockchain, a.modified_timestamp desc) = 1
