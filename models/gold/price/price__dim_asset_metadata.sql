{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'dim_asset_metadata_id',
    tags = ['core']
) }}

SELECT
    token_address,
    asset_id,
    symbol,
    NAME,
    platform AS blockchain,
    platform_id AS blockchain_id,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_asset_metadata_id AS dim_asset_metadata_id
FROM
    {{ ref('silver__complete_provider_asset_metadata') }}

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
UNION ALL
SELECT
    'usei' AS token_address,
    asset_id,
    LOWER(symbol) AS symbol,
    NAME,
    'sei-network' AS blockchain,
    'sei-network' AS blockchain_id,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_native_asset_metadata_id AS dim_asset_metadata_id
FROM
    {{ ref('silver__complete_native_asset_metadata') }}

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
