{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'ez_asset_metadata_id',
    tags = ['core']
) }}

WITH base AS (

    SELECT
        token_address,
        asset_id,
        symbol,
        NAME,
        decimals,
        blockchain,
        FALSE AS is_native,
        is_deprecated,
        inserted_timestamp,
        modified_timestamp,
        complete_token_asset_metadata_id AS ez_asset_metadata_id
    FROM
        {{ ref('silver__complete_token_asset_metadata') }}

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

qualify(ROW_NUMBER() over (PARTITION BY token_address
ORDER BY
    provider, inserted_timestamp DESC) = 1)
UNION ALL
SELECT
    'usei' AS token_address,
    asset_id,
    symbol,
    NAME,
    decimals,
    blockchain,
    TRUE AS is_native,
    is_deprecated,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id AS ez_asset_metadata_id
FROM
    {{ ref('silver__complete_native_asset_metadata') }}

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
    token_address,
    asset_id,
    COALESCE(
        b.symbol,
        C.symbol,
        A.symbol
    ) AS symbol,
    COALESCE(
        b.token_name,
        C.name,
        A.name
    ) AS NAME,
    COALESCE(
        b.decimals,
        C.decimals,
        A.decimals
    ) AS decimals,
    blockchain,
    is_native,
    is_deprecated,
    A.inserted_timestamp,
    A.modified_timestamp,
    ez_asset_metadata_id
FROM
    base A
    LEFT JOIN {{ ref('core__dim_tokens') }}
    b
    ON A.token_address = b.currency
    LEFT JOIN {{ ref('core_evm__dim_contracts') }} C
    ON A.token_address = C.address
